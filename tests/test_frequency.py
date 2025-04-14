from datetime import date, datetime, timedelta

import pytest
import polars as pl

from tsapi.frequency import infer_freq, adjust_frequency
from tsapi.model.dataset import MAX_POINTS


def test_infer_frequency_1d():
    df = pl.DataFrame(
        {
            "timestamp": [
                datetime(2021, 1, 1),
                datetime(2021, 1, 2),
                datetime(2021, 1, 3),
            ]
        }
    )

    freq = infer_freq(df["timestamp"])
    assert freq == timedelta(days=1)


def test_infer_frequency_1d_2():
    df = pl.DataFrame(
        {
            "timestamp": [
                date(2021, 1, 1),
                date(2021, 1, 2),
                date(2021, 1, 3),
            ]
        }
    )

    freq = infer_freq(df["timestamp"])
    assert freq == timedelta(days=1)


def test_infer_frequency_month():
    df = pl.DataFrame(
        {
            "timestamp": [
                datetime(2021, 2, 1),
                datetime(2021, 3, 1),
                datetime(2021, 4, 1),
                datetime(2021, 5, 1),
            ]
        }
    )

    freq = infer_freq(df["timestamp"])
    assert timedelta(days=28) <= freq <= timedelta(days=31)


def test_infer_frequency_mixed():
    df = pl.DataFrame(
        {
            "timestamp": [
                datetime(2021, 2, 1, 12, 15, 0),
                datetime(2021, 2, 1, 12, 15, 15),
                datetime(2021, 2, 1, 12, 15, 45),
                datetime(2021, 2, 1, 12, 16, 45),
                datetime(2021, 2, 1, 12, 18, 0),
            ]
        }
    )

    with pytest.raises(ValueError):
        _ = infer_freq(df["timestamp"])


def test_adjust_frequency():
    df = pl.DataFrame()
    ts = pl.datetime_range(
        datetime(2024, 1, 1),
        datetime(2024, 3, 1),
        interval='5m',
        eager=True).alias('timestamp')

    df = df.with_columns(ts)
    assert infer_freq(df["timestamp"]) == timedelta(minutes=5)

    adjusted_df = adjust_frequency(df, "timestamp")
    assert len(adjusted_df) < len(df)
    assert len(adjusted_df) <= MAX_POINTS
