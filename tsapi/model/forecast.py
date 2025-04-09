from pydantic import BaseModel

from tsapi.model.time_series import TimeRecord


class ForecastResponse(BaseModel):
    forecast: list[TimeRecord]
    metadata: dict[str, str] = {}
    model: str = "default"
    model_version: str = "1.0.0"
    error: str | None = None


class ForecastRequest(BaseModel):
    opset_id: str
    series_id: str
    horizon: int = 10
    model: str = "default"
    model_version: str = "1.0.0"
