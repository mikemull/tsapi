import asyncio

import polars as pl

from tsapi.mongo_client import MongoClient
from tsapi.model.dataset import DataSet
from main import settings


def add_dataset(name, description, file_path):
    df = pl.read_parquet(file_path)

    dataset = DataSet.from_dataframe(df, name, description)

    lup = asyncio.new_event_loop()
    client = MongoClient()
    lup.run_until_complete(client.insert_dataset(dataset.model_dump()))


def delete_dataset(dataset_id):
    lup = asyncio.new_event_loop()
    client = MongoClient(settings)
    lup.run_until_complete(client.delete_dataset(dataset_id))


def delete_dataset_by_name(name):
    lup = asyncio.new_event_loop()
    client = MongoClient(settings)
    dataset = lup.run_until_complete(client.get_dataset_by_name(name))
    lup.run_until_complete(client.delete_dataset(dataset['id']))


def dataset_info(path):
    df = pl.read_parquet(path)
    print(df.describe())
