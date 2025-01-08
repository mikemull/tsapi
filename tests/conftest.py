import pytest
import pytest_asyncio

from pydantic_settings import BaseSettings

from tsapi.mongo_client import MongoClient


@pytest.fixture(scope="session")
def config():
    class Settings(BaseSettings):
        mdb_host: str = "localhost"
        mdb_port: int = 27017
        mdb_name: str = "test_tsapidb"

    return Settings()


@pytest_asyncio.fixture(autouse=True)
async def run_around_tests(config):
    yield
    client = MongoClient(config)
    await client.client.drop_database("test_tsapidb")
