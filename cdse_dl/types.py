"""Types."""

from datetime import datetime
from typing import Iterator, Tuple, Union

from shapely.geometry.base import BaseGeometry

DatetimeOrTimestamp = Union[datetime, str]
DatetimeLike = Union[
    DatetimeOrTimestamp,
    Tuple[DatetimeOrTimestamp, DatetimeOrTimestamp],
    list[DatetimeOrTimestamp],
    Iterator[DatetimeOrTimestamp],
]
GeometryLike = Union[str, BaseGeometry]
