from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Annotated


from tsapi.model.dataset import load_electricity_data, save_dataset_source

from tsapi.model.dataset import DataSet
from tsapi.model.time_series import TimePoint, TimeSeries, TimeRecord


DATASETS = [
        DataSet(
            id=1,
            name="electricity",
            description="Electricity consumption data",
            num_series=370,
            max_length=140256,
            series_cols=[f"MT_{i:03}" for i in range(1, 10)],
            timestamp_cols=["timestamp"],
            file_name="electricityloaddiagrams20112014.parquet"
        ),
        DataSet(
            id=2,
            name="taxi",
            description="Taxi data",
            num_series=17,
            max_length=2_979_183,
            series_cols=['VendorID', 'passenger_count',
                        'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
                        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                        'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'Airport_fee'],
            timestamp_cols=['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
            file_name="yellow_tripdata_2024-08.parquet"
        )
    ]


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
    return DATASETS


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

    id_list = series_ids.split(',')

    # TODO: Look up dataset by series_id
    dataset = None
    for ds in DATASETS:
        if id_list[0] in ds.series_cols:
            dataset = ds
            break

    df = dataset.load(settings.data_dir)

    tsdata = []
    for x in df.slice(offset, limit).iter_rows(named=True):
        tsdata.append(
            TimeRecord(timestamp=x[dataset.tscol()], data={k: x[k] for k in id_list})
        )

    return TimeSeries(id=series_ids, name="electricity", data=tsdata)


@app.post("/files")
async def create_file(file: Annotated[bytes, File()]):
    return save_dataset_source("electricity", settings.data_dir, file)


@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile):
    return {"filename": file.filename}
