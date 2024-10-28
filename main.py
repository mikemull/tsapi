from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings, SettingsConfigDict

from tsapi.model.dataset import load_electricity_data

from model.dataset import DataSet
from model.time_series import TimePoint, TimeSeries, TimeRecord


class Settings(BaseSettings):
    app_name: str = "Time Series API"
    data_dir: str

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3000",
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
    return [
        DataSet(
            id=1,
            name="electricity",
            description="Electricity consumption data",
            num_series=370,
            max_length=140256,
            series_ids=[f"MT_{i:03}" for i in range(1, 10)]
        ),
        DataSet(
            id=2,
            name="traffic",
            description="Traffic data",
            num_series=963,
            max_length=17544,
        )
    ]


@app.get("/ts/{series_id}")
async def get_time_series(series_id: str, offset: int = 0, limit: int = 100) -> TimeSeries:
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
    df = load_electricity_data(settings.data_dir)

    tsdata = []
    for x in df.slice(offset, limit).iter_rows(named=True):
        tsdata.append(
            TimeRecord(timestamp=x['timestamp'], data={k: x[k] for k in id_list})
        )

    return TimeSeries(id=series_ids, name="electricity", data=tsdata)
