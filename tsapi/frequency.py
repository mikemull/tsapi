import math
from datetime import timedelta

import polars as pl

from tsapi.model.dataset import MAX_POINTS


def infer_freq(series):
    """Given a series that is either a date or a datetime, infer the frequency
    and return as timedelta.
    """

    # What do we want to happen here?  The situations we know about
    # 1) Timestamp evenly spaced, all good
    # 2) Timestamp not evenly spaced, but frequency is inferable (end of month)
    # 3) Most timestamps evenly spaced, but gaps
    # 4) Close, but not exact (from a sensor)
    # 5) No consistency (e.g, taxi pickup times)
    freq = None
    try:
        freq_counts = series.dt.replace_time_zone(None).sort().diff().value_counts().drop_nulls()
    except pl.exceptions.InvalidOperationError:
        freq_counts = series.dt.date().sort().diff().value_counts().drop_nulls()
    most_common_freq = freq_counts.sort('count', descending=True).head(1)[series.name][0]
    if len(freq_counts) == 1:
        freq = most_common_freq
    elif timedelta(days=28) <= most_common_freq <= timedelta(days=31):
        # This is a month (should this be relativedelta?)
        freq = timedelta(days=30)

    if freq is None:
        raise ValueError("Unable to infer frequency")

    return freq


def adjust_frequency(df: pl.DataFrame, timestamp_col: str) -> str:
    """
    Infer the frequency of a time series from the data

    :param df: DataFrame with a timestamp column
    :return: frequency string
    """
    if len(df) < MAX_POINTS:
        return df

    df = df.sort(timestamp_col)
    freq_counts = (df[timestamp_col] - df[timestamp_col].shift(1)).value_counts().drop_nulls()
    if len(freq_counts) == 1:
        max_freq = freq_counts.sort('count', descending=True).head(1)[timestamp_col][0]

        points_per_group = math.ceil(len(df) / MAX_POINTS)

        s = int((points_per_group * max_freq).total_seconds())
    else:
        time_delta_per_group = (df[timestamp_col].max() - df[timestamp_col].min()) / MAX_POINTS
        s = int(time_delta_per_group.total_seconds())

    return df.group_by_dynamic(timestamp_col, every=f'{s}s').agg(pl.all().mean())
