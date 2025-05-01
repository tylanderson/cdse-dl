"""Types."""

from collections.abc import Iterator
from datetime import datetime
from typing import Union

from shapely.geometry.base import BaseGeometry

DatetimeOrTimestamp = Union[datetime, str]
DatetimeLike = Union[
    DatetimeOrTimestamp,
    tuple[DatetimeOrTimestamp, DatetimeOrTimestamp],
    list[DatetimeOrTimestamp],
    Iterator[DatetimeOrTimestamp],
]
GeometryLike = Union[str, BaseGeometry]
