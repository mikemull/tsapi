from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Annotated


from tsapi.model.dataset import (
    DataSet, parse_timeseries_descriptor, infer_frequency, load_electricity_data, save_dataset_source
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
    return {"message": "Hello World"}


@app.get("/datasets")
async def get_datasets() -> list[DataSet]:
    return await MongoClient().get_datasets()


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
    print(dataset.tscol)
    df = infer_frequency(dataset.load(settings.data_dir), dataset.tscol)
    print(df)

    tsdata = []
    for x in df.slice(offset, limit).iter_rows(named=True):
        tsdata.append(
            TimeRecord(timestamp=x[dataset.tscol], data={k: x[k] for k in ts_list})
        )

    return TimeSeries(id=series_ids, name="electricity", data=tsdata)


@app.post("/files")
async def create_file(name: Annotated[str, File()], file: Annotated[bytes, File()]):
    dataset = save_dataset_source(name, settings.data_dir, file)
    id = await MongoClient().insert_dataset(dataset.model_dump())
    return id


@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile):
    return {"filename": file.filename}
