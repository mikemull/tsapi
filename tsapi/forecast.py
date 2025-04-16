import augurs as aug
import polars as pl

from tsapi.frequency import infer_freq


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


