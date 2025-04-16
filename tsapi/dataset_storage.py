import asyncio
import functools
import os

import polars as pl


async def load_async(file_path: str) -> pl.DataFrame:
    """Reads a Parquet file asynchronously using Polars."""
    loop = asyncio.get_running_loop()
    # Run the blocking read_parquet in a separate thread
    df = await loop.run_in_executor(None, pl.read_parquet, file_path)
    return df


async def load_csv_async(file_path: str) -> pl.DataFrame:
    """Reads a CSV file asynchronously using Polars."""
    loop = asyncio.get_running_loop()
    # Run the blocking read_csv in a separate thread
    kwargs = {
        "has_header": True,
        "try_parse_dates": True,
    }
    rd_csv = functools.partial(pl.read_csv, file_path, **kwargs)
    df = await loop.run_in_executor(None, rd_csv)
    return df


async def delete_dataset_from_storage(file_path: str, logger):
    """
    Delete a dataset from storage
    """
    try:
        await asyncio.to_thread(os.remove, file_path)
        logger.info(f"File '{file_path}' deleted successfully.")
    except FileNotFoundError:
        logger.info(f"Error: File '{file_path}' not found.")
