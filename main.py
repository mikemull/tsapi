from typing import Annotated, Any

import environ
from fastapi import FastAPI, File, UploadFile, HTTPException, Depends
from fastapi.responses import PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings, SettingsConfigDict


from tsapi.model.dataset import (
    DataSet, OperationSet, parse_timeseries_descriptor, adjust_frequency, load_electricity_data, save_dataset_source
)

from tsapi.model.time_series import TimePoint, TimeSeries, TimeRecord
from tsapi.mongo_client import MongoClient


class Settings(BaseSettings):
    app_name: str = "Time Series API"
    data_dir: str
    secrets_dir: str = "/var/secrets"
    secrets: Any = None

    mdb_user: str = "tsapiuser"
    mdb_host: str = "localhost"
    mdb_port: int = 27017
    mdb_name: str = "tsapidb"
    mdb_scheme: str = "mongodb"
    mdb_options: str = ""

    model_config = SettingsConfigDict(env_file=".env")

    @property
    def mdb_url(self):
        """MongoDB connection URL"""
        # NB: Leaving out port for now because mongodb+srv can't use it.
        url = f"{self.mdb_scheme}://{self.mdb_user}:{self.secrets.mdb_password}@{self.mdb_host}/{self.mdb_name}"

        if self.mdb_options:
            url += f"?{self.mdb_options}"

        return url


settings = Settings()

# Kinda silly maybe, but i like how environ does secrets.  Maybe should just
# ditch pydantic for settings?
file_secrets = environ.secrets.DirectorySecrets.from_path(settings.secrets_dir)


@environ.config
class SecretConfig:
    mdb_password = file_secrets.secret()


settings.secrets = SecretConfig.from_environ()


app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_settings():
    curr_settings = Settings()
    curr_settings.secrets = SecretConfig.from_environ()
    return curr_settings


@app.get("/")
async def root():
    return {"message": "This is the Time Series API"}


@app.get("/health", response_class=PlainTextResponse)
async def root():
    return "OK"


@app.get("/tsapi/v1/datasets")
async def get_datasets(config: Settings = Depends(get_settings)) -> list[DataSet]:
    return await MongoClient(config).get_datasets()


@app.get("/tsapi/v1/datasets/{dataset_id}")
async def get_dataset(dataset_id: str, config: Settings = Depends(get_settings)) -> DataSet:
    return await MongoClient(config).get_dataset(dataset_id)


@app.post("/tsapi/v1/opsets")
async def create_opset(opset: OperationSet, config: Settings = Depends(get_settings)):
    opset_id = await MongoClient(config).insert_opset(opset.model_dump())
    opset.id = opset_id
    return opset


@app.put("/tsapi/v1/opsets/{opset_id}")
async def update_opset(opset_id: str, opset: OperationSet) -> OperationSet:
    opset = await MongoClient(settings).update_opset(opset_id, opset.model_dump())
    if opset is None:
        raise HTTPException(status_code=404, detail="Opset not found")
    return opset


@app.get("/tsapi/v1/opsets/{opset_id}")
async def get_opset(opset_id: str) -> OperationSet:
    opset = await MongoClient(settings).get_opset(opset_id)
    return opset


@app.get("/tsapi/v1/ts/{series_id}")
async def get_time_series(series_id: str, offset: int = 0, limit: int = 100, ts_col=None) -> TimeSeries:
    df = load_electricity_data(settings.data_dir)

    tsdata = []
    for x in df.slice(offset, limit).iter_rows(named=True):
        tsdata.append(TimePoint(timestamp=x['timestamp'], x=x[series_id]))

    return TimeSeries(id=series_id, name="electricity",
                      data=tsdata)


@app.get("/tsapi/v1/tsm/{series_ids}")
async def get_multiple_time_series(series_ids: str, offset: int = 0, limit: int = 100) -> TimeSeries:

    dataset_id, ts_list = parse_timeseries_descriptor(series_ids)

    dataset_data = await MongoClient(settings).get_dataset(dataset_id)
    dataset = DataSet(**dataset_data)
    df = dataset.load(settings.data_dir).slice(offset, limit)
    df_adj = adjust_frequency(df, dataset.tscol)

    tsdata = []
    for x in df_adj.iter_rows(named=True):
        tsdata.append(
            TimeRecord(timestamp=x[dataset.tscol], data={k: x[k] for k in ts_list})
        )

    return TimeSeries(id=series_ids, name="electricity", data=tsdata)


@app.get("/tsapi/v1/tsop/{opset_id}")
async def get_op_time_series(
        opset_id: str, offset: int = 0, limit: int = 100, config: Settings = Depends(get_settings)
) -> TimeSeries:
    opset = await MongoClient(config).get_opset(opset_id)
    opset = OperationSet(**opset)

    dataset_data = await MongoClient(settings).get_dataset(opset.dataset_id)
    dataset = DataSet(**dataset_data)
    df = dataset.load(settings.data_dir).slice(offset, limit)
    df_adj = adjust_frequency(df, dataset.tscol)

    tsdata = []
    for x in df_adj.iter_rows(named=True):
        tsdata.append(TimeRecord(timestamp=x[dataset.tscol], data={k: x[k] for k in opset.plot}))

    return TimeSeries(id=opset_id, name="electricity", data=tsdata)


@app.post("/tsapi/v1/files")
async def create_file(name: Annotated[str, File()], file: Annotated[bytes, File()]):
    dataset = save_dataset_source(name, settings.data_dir, file)
    id = await MongoClient(settings).insert_dataset(dataset.model_dump())
    return id


@app.post("/tsapi/v1/uploadfile/")
async def create_upload_file(file: UploadFile):
    return {"filename": file.filename}
