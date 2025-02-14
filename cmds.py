import asyncio
import os
import polars as pl

from tsapi.mongo_client import MongoClient
from tsapi.model.dataset import parse_dataset


def add_dataset(name, description, file_path):
    df = pl.read_parquet(file_path)

    dataset = parse_dataset(df, name, description, os.path.basename(file_path))

    lup = asyncio.new_event_loop()
    client = MongoClient()
    lup.run_until_complete(client.insert_dataset(dataset.model_dump()))


def dataset_info(path):
    df = pl.read_parquet(path)
    print(df.describe())


if __name__ == "__main__":
    import sys
    dataset_info(sys.argv[1])
