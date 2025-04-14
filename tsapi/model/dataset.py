import asyncio
import io
import os
import re
from typing import Optional

import polars as pl
from pydantic import BaseModel

from tsapi.errors import TsApiNoTimestampError

MAX_POINTS = 10000  # TODO: make this a setting


class DatasetRequest(BaseModel):
    name: str
    upload_type: str


class OperationSet(BaseModel):
    id: str
    dataset_id: str
    plot: list[str] = []
    offset: int = 0
    limit: int = 1000
    dependent: Optional[str] = None


class DataSet(BaseModel):
    id: str
    name: str
    description: str = None
    num_series: int
    max_length: int
    series_cols: list[str] = []
    timestamp_cols: list[str] = []
    other_cols: list[str] = []
    file_name: str
    ops: list[OperationSet] = []

    def load(self, data_dir) -> pl.DataFrame:
        return pl.read_parquet((os.path.join(data_dir, self.file_name)))

    async def load_async(self, data_dir: str) -> pl.DataFrame:
        """Reads a Parquet file asynchronously using Polars."""
        loop = asyncio.get_running_loop()
        # Run the blocking read_parquet in a separate thread
        df = await loop.run_in_executor(None, self.load, data_dir)
        return df

    @property
    def tscol(self):
        return self.timestamp_cols[0]

    @staticmethod
    def from_dataframe(dataframe: pl.DataFrame, name: str):
        """ Extract metadata from columns and dtypes and also rename empty columns """

        series = []
        times = []
        others = []

        for col, value in dataframe.schema.items():
            if value.is_numeric():
                series.append(col)
            elif value.is_temporal():
                times.append(col)
            else:
                others.append(col)

        if len(times) == 0:
            raise TsApiNoTimestampError("No timestamp columns found")

        return DataSet(
            id="abc",
            name=name,
            description='',
            num_series=len(series),
            max_length=len(dataframe),
            series_cols=series,
            timestamp_cols=times,
            other_cols=others,
            file_name=f'{name}.parquet'
        )


def rename_blank_columns(df: pl.DataFrame):
    iblank = 0
    for col in df.columns:
        if col.strip() == '':
            df = df.rename({col: f'Unk:{iblank}'})
            iblank += 1

    return df


def build_dataset(name: str, data_dir: str) -> DataSet:
    df = pl.read_parquet((os.path.join(data_dir, f'{name}.parquet')))
    return DataSet.from_dataframe(df, name)


def import_dataset(name: str, data_dir: str) -> DataSet:
    """
    Import a dataset from a CSV file and convert it to parquet format.
    This function will also rename any blank columns in the dataframe.
    """
    source_file_name = os.path.join(data_dir, f'{name}.csv')
    df = pl.read_csv(source_file_name, has_header=True, try_parse_dates=True)
    df = rename_blank_columns(df)

    df.write_parquet(os.path.join(data_dir, f'{name}.parquet'))

    return DataSet.from_dataframe(df, name)


def store_dataset(name: str, data_dir: str, data: bytes, upload_type: str, logger):
    try:
        if upload_type == 'add':
            df = pl.read_parquet(io.BytesIO(data))
            df.write_parquet(os.path.join(data_dir, f'{name}.parquet'))
        else:
            df = pl.read_csv(io.BytesIO(data))
            df.write_csv(os.path.join(data_dir, f'{name}.csv'))
    except Exception as e:
        logger.error(f"Error reading data: {e}")
        raise e


def save_dataset(name: str, data_dir: str, data: bytes, logger):
    try:
        df_parquet = pl.read_parquet(io.BytesIO(data))
    except Exception as e:
        logger.error(f"Error reading parquet data: {e}")
        df_parquet = None

    df_parquet = rename_blank_columns(df_parquet)
    dataset = DataSet.from_dataframe(df_parquet, name)
    df_parquet.write_parquet(os.path.join(data_dir, dataset.file_name))

    return dataset


def save_dataset_source(name: str, data_dir: str, data: bytes):
    """ When a new CSV files is imported, this will save the original and
        then attempt to convert it into a parquet file.
    """
    source_file_name = os.path.join(data_dir, f'{name}_source.csv')
    with open(source_file_name, 'wb') as f:
        f.write(data)

    df = pl.read_csv(source_file_name, has_header=True, try_parse_dates=True)

    df = rename_blank_columns(df)
    dataset = DataSet.from_dataframe(df, name)
    df.write_parquet(os.path.join(data_dir, dataset.file_name))

    return dataset


def parse_timeseries_descriptor(descriptor: str):
    """
    Parse a descriptor string into dataset and series names

    The descriptor will be in the form:
    <dataset_name>:[<series1>,<series2>,...]

    :param descriptor:
    :return: list of tuples of dataset name and series names
    """
    m = re.match(r'(.+):(.+)', descriptor)
    if m:
        return m.group(1), m.group(2).split(',')
    else:
        raise ValueError("Invalid descriptor")


def load_electricity_data(data_dir) -> pl.DataFrame:
    return pl.read_parquet(os.path.join(data_dir, 'electricityloaddiagrams20112014.parquet'))


def load_electricity_data_source(data_dir) -> pl.DataFrame:
    """
    Load electricity data
    """
    df = pl.read_csv(
        os.path.join(data_dir, 'LD2011_2014.txt'),
        separator=';',
        has_header=True,
        decimal_comma=True,
        schema_overrides=pl.Schema({f'MT_{d:03}': pl.Float32() for d in range(1, 371)}),
        try_parse_dates=True)

    return df


async def delete_dataset_from_storage(dataset: DataSet, data_dir: str, logger):
    """
    Delete a dataset from storage
    """
    try:
        file_path = os.path.join(data_dir, f'{dataset.name}.parquet')
        await asyncio.to_thread(os.remove, file_path)
        logger.info(f"File '{file_path}' deleted successfully.")
    except FileNotFoundError:
        logger.info(f"Error: File '{file_path}' not found.")
