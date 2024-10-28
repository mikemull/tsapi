from datetime import datetime
from typing import Union, Dict
from pydantic import BaseModel


class TimePoint(BaseModel):
    timestamp: datetime
    x: float


class TimeRecord(BaseModel):
    timestamp: datetime
    data: Dict[str, float]


#
# TODO: I want three options i think:
#  (1) data is just a list of floats,
#  (2) data is a list of TimePoint,
#  (3) data is a list of float arrays (TimePoint has a dict of lists?)


class TimeSeries(BaseModel):
    id: str
    name: str
    # data: list[float]
    data: Union[list[float], list[TimePoint], list[TimeRecord]]


class TimeSeriesSet(BaseModel):
    series: list[TimeSeries]
