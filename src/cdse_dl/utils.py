"""General utils."""

from collections.abc import Iterable
from datetime import datetime

from dateutil.parser import parse as dt_parse

from cdse_dl.odata.filter import make_datetime_utc
from cdse_dl.types import DatetimeLike


def parse_datetime_to_components(
    value: DatetimeLike,
) -> list[datetime | None]:
    """Convert DatetimeLike to list of datetimes.

    Args:
        value (DatetimeLike): DatetimeLike to convert

    Raises:
        Exception: too many or too few datetime components
        Exception: double open-ended datetime components

    Returns:
        list[datetime | None]: datetime components
    """
    components: Iterable[str | datetime | None]
    if isinstance(value, datetime):
        components = [make_datetime_utc(value), None]
    elif isinstance(value, str):
        components = list(value.split("/"))
        if len(components) == 1:
            components.append(None)
    else:
        components = value

    datetime_components: list[datetime | None] = []
    for component in components:
        if component:
            if isinstance(component, str):
                component = dt_parse(component)
            datetime_components.append(make_datetime_utc(component))
        else:
            datetime_components.append(None)

    if len(datetime_components) != 2:
        raise Exception(
            "too many/few datetime components "
            f"(expected=2, actual={len(list(components))}): {list(components)}"
        )
    elif all(c is None for c in datetime_components):
        raise Exception("cannot create a double open-ended interval")

    return datetime_components
