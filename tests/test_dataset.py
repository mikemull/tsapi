from datetime import datetime

import polars as pl
import pytest

from tsapi.model.dataset import DataSet, rename_blank_columns
from tsapi.errors import TsApiNoTimestampError


@pytest.fixture()
def dataset_df():
    return pl.DataFrame(
        {
            "timestamp": [
                datetime(2021, 1, 1),
                datetime(2021, 1, 2),
                datetime(2021, 1, 3)
                ],
            "series1": [1, 2, 3],
            "series2": [4.0, 5.0, 6.0],
            "series3": ["a", "b", "c"],
            "": ["a", "b", "c"],
        }
    )


@pytest.fixture()
def dataset_df_no_time():
    return pl.DataFrame(
        {
            "series1": [1, 2, 3],
            "series2": [4.0, 5.0, 6.0],
        }
    )


def test_dataset_parse(dataset_df):
    dset = DataSet.from_dataframe(dataset_df, "test")

    assert len(dset.series_cols) == 2
    assert len(dset.timestamp_cols) == 1
    assert len(dset.other_cols) == 2
    assert dset.num_series == 2
    assert dset.max_length == 3
    assert dset.file_name == "test.parquet"


def test_rename(dataset_df):
    dataset_df = rename_blank_columns(dataset_df)

    assert 'Unk:0' in dataset_df.columns


def test_dataset_parse_no_time(dataset_df_no_time):
    with pytest.raises(TsApiNoTimestampError):
        DataSet.from_dataframe(dataset_df_no_time, "test")
