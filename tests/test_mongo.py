import pytest

from tsapi.mongo_client import MongoClient


@pytest.fixture(scope='function')
def async_mongodb(config):
    client = MongoClient(config)
    return client


@pytest.mark.asyncio()
async def test_mongodb_fixture(async_mongodb):
    docs = await async_mongodb.get_datasets()

    assert docs == []


@pytest.mark.asyncio()
async def test_insert_dataset(async_mongodb):
    doc_id = await async_mongodb.insert_dataset({"name": "test", "description": "test", "file_path": "test.parquet"})

    assert doc_id is not None

    doc = await async_mongodb.get_dataset(doc_id)
    assert doc['id'] == doc_id
