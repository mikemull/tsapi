from typing import Annotated, Any

import environ
from fastapi import FastAPI, File, HTTPException, Depends, Query, Request, status
from fastapi.responses import Response, PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings, SettingsConfigDict
import structlog


from tsapi.gcs import generate_signed_url
from tsapi.model.dataset import (
    DataSet, OperationSet, save_dataset, save_dataset_source, DatasetRequest,
    store_dataset, delete_dataset_from_storage
)
from tsapi.frequency import adjust_frequency
from tsapi.model.responses import SignedURLResponse
from tsapi.model.forecast import ForecastResponse, ForecastRequest
from tsapi.model.time_series import TimeSeries, TimeRecord
from tsapi.mongo_client import MongoClient
from tsapi.dataset_cache import DatasetCache
from tsapi.forecast import forecast
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
    try:

        if dataset_req.upload_type == 'add':
            dataset = DataSet.build(dataset_req.name, config.data_dir)
        elif dataset_req.upload_type == 'import':
            dataset = DataSet.import_csv(dataset_req.name, config.data_dir)
        else:
            raise HTTPException(status_code=400, detail="Invalid upload type")

    except TsApiNoTimestampError as e:
        logger.error("No timestamp column found", name=dataset_req.name, error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Unexpected error", name=dataset_req.name, error=str(e))
        raise HTTPException(status_code=400, detail=str(e))

    dataset_id = await MongoClient(config).insert_dataset(dataset.model_dump())
    dataset.id = dataset_id

    return dataset


@app.get("/tsapi/v1/datasets/{dataset_id}")
async def get_dataset(dataset_id: str, config: Settings = Depends(get_settings)) -> DataSet:
    return await MongoClient(config).get_dataset(dataset_id)


@app.delete("/tsapi/v1/datasets/{dataset_id}")
async def delete_dataset(dataset_id: str, config: Settings = Depends(get_settings)) -> DataSet:
    mngo_client = MongoClient(settings)
    dataset = DataSet.model_validate(await mngo_client.get_dataset(dataset_id))
    await mngo_client.delete_dataset(dataset_id)
    await delete_dataset_from_storage(dataset, config.data_dir, logger)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


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

    dataset_data = await MongoClient(settings).get_dataset(opset['dataset_id'])

    ds_cache = DatasetCache(DataSet(**dataset_data), settings, logger)
    await ds_cache.update_operation_set(OperationSet(**opset), OperationSet(**curr_opset))

    return opset


@app.get("/tsapi/v1/opsets/{opset_id}")
async def get_opset(opset_id: str) -> OperationSet:
    opset = await MongoClient(settings).get_opset(opset_id)
    return opset


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

    ds_cache = DatasetCache(dataset, config, logger)

    # Check if there's already a dataset for this opset
    dataset_df = await ds_cache.get_operation_set(opset)

    # We have to do downsampling here because it changes the number of rows
    dataset_df = adjust_frequency(dataset_df, dataset.tscol)
    logger.info("Adjusted frequency")

    tsdata = []
    for x in dataset_df.iter_rows(named=True):
        tsdata.append(TimeRecord(timestamp=x[dataset.tscol], data={k: x[k] for k in opset.series_ids}))

    logger.info("Created time series data")

    return TimeSeries(id=opset_id, name="electricity", data=tsdata)


@app.post("/tsapi/v1/forecast")
async def create_forecast(
        forecast_req: ForecastRequest,
        config: Settings = Depends(get_settings)) -> ForecastResponse:

    opset = await MongoClient(config).get_opset(forecast_req.opset_id)
    opset = OperationSet(**opset)
    logger.info('Retrieved opset', opset=opset)
    dataset_data = await MongoClient(settings).get_dataset(opset.dataset_id)
    dataset = DataSet(**dataset_data)

    ds_cache = DatasetCache(dataset, config, logger)
    # Check if there's already a dataset for this opset
    dataset_df = await ds_cache.get_operation_set(opset)

    forecast_result = forecast(
        dataset_df[forecast_req.series_id],
        dataset_df[dataset.tscol],
        horizon=forecast_req.horizon)
    return ForecastResponse(
        forecast=[TimeRecord(timestamp=t, data=data) for t, data in forecast_result],
    )


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
        # In local mode, we just return a local URL
        return SignedURLResponse(
            url=f'http://localhost:8000/tsapi/v1/upload?name={dataset_req.name}&upload_type={dataset_req.upload_type}'
        )
