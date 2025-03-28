from typing import Annotated, Any

import environ
from fastapi import FastAPI, File, HTTPException, Depends, Query, Request
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings, SettingsConfigDict
import structlog


from tsapi.gcs import generate_signed_url
from tsapi.model.dataset import (
    DataSet, OperationSet, parse_timeseries_descriptor, adjust_frequency, load_electricity_data,
    save_dataset, save_dataset_source, DatasetRequest, build_dataset, import_dataset, store_dataset
)
from tsapi.model.responses import SignedURLResponse
from tsapi.model.time_series import TimePoint, TimeSeries, TimeRecord
from tsapi.mongo_client import MongoClient
from tsapi.dataset_cache import DatasetCache
from tsapi.errors import TsApiNoTimestampError


class Settings(BaseSettings):
    env: str = "local"
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

    redis_host: str = "localhost"

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

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ]
)

logger = structlog.get_logger()


app = FastAPI()
app.logger = logger


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
async def health():
    return "OK"


@app.get("/tsapi/v1/datasets")
async def get_datasets(config: Settings = Depends(get_settings)) -> list[DataSet]:
    return await MongoClient(config).get_datasets()


@app.post("/tsapi/v1/datasets")
async def create_dataset(
        dataset_req: DatasetRequest,
        config: Settings = Depends(get_settings)
) -> DataSet:
    """
    This creates a dataset, but it presumes that a file has already been uploaded
    to GCS (see create_signed_url).  The file is then loaded into a DataSet object
    :param dataset_req:
    :param config:
    :return:
    """
    if dataset_req.upload_type == 'add':
        dataset = build_dataset(dataset_req.name, config.data_dir)
    elif dataset_req.upload_type == 'import':
        dataset = import_dataset(dataset_req.name, config.data_dir)
    else:
        raise HTTPException(status_code=400, detail="Invalid upload type")

    dataset_id = await MongoClient(config).insert_dataset(dataset.model_dump())
    dataset.id = dataset_id

    return dataset


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
    mngo_client = MongoClient(settings)

    curr_opset = await mngo_client.get_opset(opset_id)
    opset = await mngo_client.update_opset(opset_id, opset.model_dump())
    if opset is None:
        raise HTTPException(status_code=404, detail="Opset not found")
    if curr_opset['limit'] != opset['limit'] or curr_opset['offset'] != opset['offset']:
        # If the limit or offset has changed, we need to clear the dataset cache
        dscache = DatasetCache(settings)
        await dscache.client.delete(curr_opset['dataset_id'])
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
        opset_id: str, config: Settings = Depends(get_settings)
) -> TimeSeries:
    logger.info("Get time series", opset_id=opset_id)

    opset = await MongoClient(config).get_opset(opset_id)
    opset = OperationSet(**opset)
    logger.info('Retrieved opset', opset=opset)
    dataset_data = await MongoClient(settings).get_dataset(opset.dataset_id)
    dataset = DataSet(**dataset_data)

    dscache = DatasetCache(config)

    # Check if the dataset is cached
    dataset_df = await dscache.get_cached_dataset(opset.dataset_id)

    if dataset_df is None:
        logger.info("Loaded dataset", dataset=dataset, data_dir=settings.data_dir)
        dataset_df = dataset.load(settings.data_dir)
        logger.info('Loaded dataframe', rows=len(dataset_df))
        dataset_df = dataset_df.slice(opset.offset, opset.limit)
        logger.info("Sliced dataframe", rows=len(dataset_df))
        dataset_df = adjust_frequency(dataset_df, dataset.tscol)
        logger.info("Adjusted frequency")
        await dscache.cache_dataset(opset.dataset_id, dataset_df)
    else:
        logger.info("Using cached dataset", rows=len(dataset_df))

    tsdata = []
    for x in dataset_df.iter_rows(named=True):
        tsdata.append(TimeRecord(timestamp=x[dataset.tscol], data={k: x[k] for k in opset.plot}))

    logger.info("Created time series data")

    return TimeSeries(id=opset_id, name="electricity", data=tsdata)


@app.post("/tsapi/v1/files")
async def create_file(
        name: Annotated[str, File()],
        upload_type: Annotated[str, File()],
        file: Annotated[bytes, File()]
) -> DataSet:
    logger.info("Received file: ", name=name, upload_type=upload_type)

    try:
        if upload_type == "add":
            dataset = save_dataset(name, settings.data_dir, file, logger)
        elif upload_type == "import":
            dataset = save_dataset_source(name, settings.data_dir, file)
        else:
            raise HTTPException(status_code=400, detail="Invalid upload type")

        dataset_id = await MongoClient(settings).insert_dataset(dataset.model_dump())
    except TsApiNoTimestampError as e:
        logger.error("No timestamp column found", name=name, error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Unexpected error", name=name, error=str(e))
        raise HTTPException(status_code=400, detail=str(e))

    return await MongoClient(settings).get_dataset(dataset_id)


@app.put("/tsapi/v1/upload")
async def store_file(
        request: Request,
        name: str = Query(...),
        upload_type: str = Query(...)
) -> DataSet:
    data = await request.body()
    store_dataset(name, settings.data_dir, data, upload_type, logger)

    return JSONResponse(content={"message": "File stored successfully"})


@app.post("/tsapi/v1/signed-url")
async def create_signed_url(
        dataset_req: DatasetRequest, config: Settings = Depends(get_settings)
) -> SignedURLResponse:
    """
    Create a signed URL for uploading a file to Google Cloud Storage.
    """
    file_type = 'parquet' if dataset_req.upload_type == 'add' else 'csv'

    if settings.env != 'local':
        signed_url = generate_signed_url(
            bucket_name='tsnext_bucket',
            blob_name=f'datasets/{dataset_req.name}.{file_type}',
            expiration_minutes=5
        )

        return SignedURLResponse(url=signed_url)
    else:
        # In local mode, we just return a dummy URL
        return SignedURLResponse(
            url=f'http://localhost:8000/tsapi/v1/upload?name={dataset_req.name}&upload_type={dataset_req.upload_type}'
        )
