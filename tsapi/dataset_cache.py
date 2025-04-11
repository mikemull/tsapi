import io

import redis.asyncio as redis
import polars as pl

from tsapi.model.dataset import DataSet, OperationSet


class DatasetCache:

    def __init__(self, dataset: DataSet, settings, logger):
        # TODO: pool?
        self.client = redis.Redis(host=settings.redis_host, port=6379, db=0)
        self.logger = logger
        self.settings = settings
        self.dataset = dataset

    async def get_cached_dataset(self, dataset_key) -> pl.DataFrame:
        """
        Retrieve a cached dataset by its ID or an opset ID.
        """
        try:
            cached_data = await self.client.get(dataset_key)
            if cached_data is None:
                return None

            datasetio = io.BytesIO(cached_data)
            dataframe = pl.read_ipc(datasetio)
            return dataframe
        except Exception as e:
            self.logger.error(f"Error retrieving cached dataset: {e}")
            return None

    async def cache_dataset(self, dataset_key: str, dataframe: pl.DataFrame):
        """
        Cache a dataset by its ID or and opset ID.
        """
        try:
            datasetio = io.BytesIO()
            dataframe.write_ipc(datasetio, compression='zstd')
            # Cache with an expiration time of 1 hour
            await self.client.set(dataset_key, datasetio.getvalue(), ex=3600)
        except Exception as e:
            self.logger.error(f"Error caching dataset: {e}")

    async def get_operation_set(self, opset: OperationSet) -> pl.DataFrame:
        """
        Retrieve a dataset by its ID or opset ID.
        """
        dataset_df = await self.get_cached_dataset(opset.id)

        if dataset_df is None:
            dataset_df = await self.get_cached_dataset(self.dataset.id)
            if dataset_df is None:
                # Load the dataset from the source
                self.logger.info("Loading dataset from source")
                dataset_df = await self.dataset.load_async(self.settings.data_dir)
                self.logger.info('Loaded dataframe', rows=len(dataset_df))
                await self.cache_dataset(opset.dataset_id, dataset_df)

            dataset_df = dataset_df.slice(opset.offset, opset.limit)
            self.logger.info("Sliced dataframe", rows=len(dataset_df))
            await self.cache_dataset(opset.id, dataset_df)
        else:
            self.logger.info("Using cached dataset", rows=len(dataset_df))

        return dataset_df
