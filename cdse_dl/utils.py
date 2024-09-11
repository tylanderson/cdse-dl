"""General utils."""

from datetime import datetime
from typing import List, Optional, Sequence, Union

from dateutil.parser import parse as dt_parse

from cdse_dl.odata.filter import make_datetime_utc
from cdse_dl.types import DatetimeLike


def parse_datetime_to_components(
    value: DatetimeLike,
) -> List[Optional[datetime]]:
    """Convert DatetimeLike to list of datetimes.

    Args:
        value (DatetimeLike): DatetimeLike to convert

    Returns:
        Optional[List[Optional[datetime]]]: datetime components
    """
    components: Sequence[Union[str, datetime, None]]
    if isinstance(value, datetime):
        components = [make_datetime_utc(value), None]
    elif isinstance(value, str):
        components = list(value.split("/"))
        if len(components) == 1:
            components.append(None)  # type: ignore
    else:
        components = value  # type: ignore

    datetime_components: List[Optional[datetime]] = []
    for component in components:
        if component:
            if isinstance(component, str):
                component = dt_parse(component)
            datetime_components.append(make_datetime_utc(component))
        else:
            datetime_components.append(None)

    if all(c is None for c in datetime_components):
        raise Exception("cannot create a double open-ended interval")
    elif len(datetime_components) != 2:
        raise Exception(
            "too many/few datetime components "
            f"(expected=2, actual={len(components)}): {components}"
        )

    return datetime_components
