from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Annotated


from tsapi.model.dataset import (
    DataSet, OperationSet, parse_timeseries_descriptor, adjust_frequency, load_electricity_data, save_dataset_source
)

from tsapi.model.time_series import TimePoint, TimeSeries, TimeRecord
from tsapi.mongo_client import MongoClient


class Settings(BaseSettings):
    app_name: str = "Time Series API"
    data_dir: str

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()

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


@app.get("/")
async def root():
    return {"message": "This is the Time Series API"}


@app.get("/datasets")
async def get_datasets() -> list[DataSet]:
    return await MongoClient().get_datasets()


@app.get("/datasets/{dataset_id}")
async def get_dataset(dataset_id: str) -> DataSet:
    return await MongoClient().get_dataset(dataset_id)


@app.post("/opsets")
async def create_opset(opset: OperationSet):
    opset_id = await MongoClient().insert_opset(opset.model_dump())
    opset.id = opset_id
    return opset


@app.put("/opsets/{opset_id}")
async def update_opset(opset_id: str, opset: OperationSet) -> OperationSet:
    opset = await MongoClient().update_opset(opset_id, opset.model_dump())
    if opset is None:
        raise HTTPException(status_code=404, detail="Opset not found")
    return opset


@app.get("/opsets/{opset_id}")
async def get_opset(opset_id: str) -> OperationSet:
    opset = await MongoClient().get_opset(opset_id)
    return opset


@app.get("/ts/{series_id}")
async def get_time_series(series_id: str, offset: int = 0, limit: int = 100, ts_col=None) -> TimeSeries:
    df = load_electricity_data(settings.data_dir)

    tsdata = []
    for x in df.slice(offset, limit).iter_rows(named=True):
        tsdata.append(TimePoint(timestamp=x['timestamp'], x=x[series_id]))

    return TimeSeries(id=series_id, name="electricity",
                      data=tsdata)


@app.get("/tsm/{series_ids}")
async def get_multiple_time_series(series_ids: str, offset: int = 0, limit: int = 100) -> TimeSeries:

    dataset_id, ts_list = parse_timeseries_descriptor(series_ids)

    dataset_data = await MongoClient().get_dataset(dataset_id)
    dataset = DataSet(**dataset_data)
    df = dataset.load(settings.data_dir).slice(offset, limit)
    df_adj = adjust_frequency(df, dataset.tscol)

    tsdata = []
    for x in df_adj.iter_rows(named=True):
        tsdata.append(
            TimeRecord(timestamp=x[dataset.tscol], data={k: x[k] for k in ts_list})
        )

    return TimeSeries(id=series_ids, name="electricity", data=tsdata)


@app.get("/tsop/{opset_id}")
async def get_op_time_series(opset_id: str, offset: int = 0, limit: int = 100) -> TimeSeries:
    opset = await MongoClient().get_opset(opset_id)
    opset = OperationSet(**opset)

    dataset_data = await MongoClient().get_dataset(opset.dataset_id)
    dataset = DataSet(**dataset_data)
    df = dataset.load(settings.data_dir).slice(offset, limit)
    df_adj = adjust_frequency(df, dataset.tscol)

    tsdata = []
    for x in df_adj.iter_rows(named=True):
        tsdata.append(TimeRecord(timestamp=x[dataset.tscol], data={k: x[k] for k in opset.plot}))

    return TimeSeries(id=opset_id, name="electricity", data=tsdata)


@app.post("/files")
async def create_file(name: Annotated[str, File()], file: Annotated[bytes, File()]):
    dataset = save_dataset_source(name, settings.data_dir, file)
    id = await MongoClient().insert_dataset(dataset.model_dump())
    return id


@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile):
    return {"filename": file.filename}
