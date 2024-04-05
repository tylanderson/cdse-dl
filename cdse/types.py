"""Types."""

from datetime import datetime
from typing import Iterator, List, Tuple, Union

from shapely.geometry.base import BaseGeometry

DatetimeOrTimestamp = Union[datetime, str]
DatetimeLike = Union[
    DatetimeOrTimestamp,
    Tuple[DatetimeOrTimestamp, DatetimeOrTimestamp],
    List[DatetimeOrTimestamp],
    Iterator[DatetimeOrTimestamp],
]
GeometryLike = Union[str, BaseGeometry]
