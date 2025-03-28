import io

import redis.asyncio as redis
import polars as pl


class DatasetCache:

    def __init__(self, settings, logger):
        self.client = redis.Redis(host=settings.redis_host, port=6379, db=0)
        self.logger = logger

    async def get_cached_dataset(self, dataset_id) -> pl.DataFrame:
        """
        Retrieve a cached dataset by its ID.
        """
        try:
            cached_data = await self.client.get(dataset_id)
            if cached_data is None:
                return None

            datasetio = io.BytesIO(cached_data)
            dataframe = pl.read_ipc(datasetio)
            return dataframe
        except Exception as e:
            self.logger.error(f"Error retrieving cached dataset: {e}")
            return None

    async def cache_dataset(self, dataset_id: str, dataframe: pl.DataFrame):
        """
        Cache a dataset by its ID.
        """
        try:
            datasetio = io.BytesIO()
            dataframe.write_ipc(datasetio, compression='lz4')
            await self.client.set(dataset_id, datasetio.getvalue(), ex=3600)
        except Exception as e:
            self.logger.error(f"Error caching dataset: {e}")
