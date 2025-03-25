import io

import redis.asyncio as redis
import polars as pl


class DatasetCache:

    def __init__(self, settings):
        self.client = redis.Redis(host=settings.redis_host, port=6379, db=0)

    async def get_cached_dataset(self, dataset_id) -> pl.DataFrame:
        """
        Retrieve a cached dataset by its ID.
        """
        cached_data = await self.client.get(dataset_id)
        if cached_data is None:
            return None

        datasetio = io.BytesIO(cached_data)
        dataframe = pl.read_ipc(datasetio)
        return dataframe

    async def cache_dataset(self, dataset_id: str, dataframe: pl.DataFrame):
        """
        Cache a dataset by its ID.
        """

        datasetio = io.BytesIO()
        dataframe.write_ipc(datasetio, compression='lz4')
        await self.client.set(dataset_id, datasetio.getvalue(), ex=3600)
