import augurs as aug
import polars as pl


def forecast(series, timestamp, horizon=10):
    if series.dtype != pl.Float64:
        series = series.cast(pl.Float64)

    y = series.to_numpy()
    periods = [3, 4]
    model = aug.MSTL.ets(periods)
    model.fit(y)
    predictions = model.predict(horizon, level=0.95)

    pred_records = []
    freq = infer_freq(timestamp)
    for i in range(horizon):
        pred_records.append(
            (
                timestamp[-1] + (i + 1) * freq,
                {
                    "point": predictions.point()[i],
                    "lower": predictions.lower()[i],
                    "upper": predictions.upper()[i]
                }
            )
        )

    return pred_records

# y = df['price'].to_numpy()
# periods = [3, 4]
# # Use an AutoETS trend forecaster
# model = aug.MSTL.ets(periods)
# model.fit(y)
# out_of_sample = model.predict(10, level=0.95)
# print(out_of_sample.point())
# print(out_of_sample.lower())
# print(out_of_sample.upper())
# in_sample = model.predict_in_sample(level=0.95)
#
def infer_freq(series):
    """Given a series that is either a date or a datetime, infer the frequency
    and return as timedelta.
    """
    try:
        freq = series.dt.datetime().sort().diff().mode()[0]
    except pl.exceptions.ComputeError:
        freq = series.dt.date().sort().diff().mode()[0]

    if freq is None:
        raise ValueError("Unable to infer frequency")

    return freq
