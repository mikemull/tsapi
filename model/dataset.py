import os
import polars as pl
from pydantic import BaseModel


class DataSet(BaseModel):
    id: int
    name: str
    description: str = None
    num_series: int
    max_length: int
    series_cols: list[str] = []
    timestamp_cols: list[str] = []
    file_name: str

    def load(self, data_dir):
        return pl.read_parquet((os.path.join(data_dir, self.file_name)))

    def tscol(self):
        return self.timestamp_cols[0]


def save_dataset_source(name: str, data_dir: str, data: bytes):
    source_file_name = os.path.join(data_dir, f'{name}_source.csv')
    with open(source_file_name, 'wb') as f:
        f.write(data)

    df = pl.read_csv(source_file_name, has_header=True, try_parse_dates=True)

    dataset_file_name = f'{name}.parquet'
    df.write_parquet(os.path.join(data_dir, dataset_file_name))

    return parse_dataset(df, name, '', dataset_file_name)


def parse_dataset(
        dataframe: pl.DataFrame,
        name: str,
        description: str,
        dataset_file_name: str
) -> DataSet:
    """ Maybe a method of dataset? """

    series = []
    times = []

    for k, v in dataframe.schema.items():
        if v.is_numeric():
            series.append(k)
        elif v.is_temporal():
            times.append(k)

    if len(times) == 0:
        raise ValueError("No timestamp columns found")

    return DataSet(
        id=1,
        name=name,
        description=description,
        num_series=len(series),
        max_length=len(dataframe),
        series_cols=series,
        timestamp_cols=times,
        file_name=dataset_file_name
    )


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
