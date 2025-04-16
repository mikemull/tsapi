import io
import os
from typing import Optional, Self

import polars as pl
from pydantic import BaseModel

from tsapi.errors import TsApiNoTimestampError
from tsapi.frequency import check_time_series
from tsapi.dataset_storage import load_async, load_csv_async, delete_dataset_from_storage


class DatasetRequest(BaseModel):
    name: str
    upload_type: str


class OperationSet(BaseModel):
    id: str
    dataset_id: str
    series_ids: list[str] = []
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
    conditions: list[str] = []

    def load(self, data_dir) -> pl.DataFrame:
        return pl.read_parquet(os.path.join(data_dir, self.file_name))

    async def load_async(self, data_dir: str) -> pl.DataFrame:
        """Reads a Parquet file asynchronously using Polars."""
        df = await load_async(os.path.join(data_dir, self.file_name))
        return df

    async def delete(self, data_dir, logger):
        return await delete_dataset_from_storage(os.path.join(data_dir, f'{self.name}.parquet'), logger)

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
            file_name=f'{name}.parquet',
            conditions=check_time_series(dataframe[times[0]])
        )

    @classmethod
    async def build(cls, name: str, data_dir: str) -> Self:
        """
        Build a DataSet object from a parquet file.
        """
        df = await load_async(os.path.join(data_dir, f'{name}.parquet'))
        return DataSet.from_dataframe(df, name)

    @classmethod
    async def import_csv(cls, name: str, data_dir: str) -> Self:
        """
        Import a dataset from a CSV file and convert it to parquet format.
        This function will also rename any blank columns in the dataframe.
        """
        source_file_name = os.path.join(data_dir, f'{name}.csv')
        df = await load_csv_async(source_file_name)
        df = rename_blank_columns(df)

        df.write_parquet(os.path.join(data_dir, f'{name}.parquet'))

        return DataSet.from_dataframe(df, name)


def rename_blank_columns(df: pl.DataFrame):
    iblank = 0
    for col in df.columns:
        if col.strip() == '':
            df = df.rename({col: f'Unk:{iblank}'})
            iblank += 1

    return df


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
