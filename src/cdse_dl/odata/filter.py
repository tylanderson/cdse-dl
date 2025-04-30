"""OData Filter creation and helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Self

from dateutil import tz


def make_datetime_utc(dt: datetime) -> datetime:
    """Ensure a datetime is in UTC.

    If no tzinfo, it is assumed to be UTC

    Args:
        dt (datetime): datetime

    Returns:
        datetime: datetime in utc
    """
    # Check if the datetime is naive (has no timezone)
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        # Make the datetime timezone-aware, assuming it's in UTC
        return dt.replace(tzinfo=tz.UTC)
    else:
        # It's already timezone-aware, return as is
        return dt.astimezone(tz.UTC)


class Filter:
    """Default Filter."""

    def __init__(self, filter_string: str, is_contains: bool = False):
        """Create Filter.

        Args:
            filter_string (str): filter string
            is_contains (bool, optional): is a contains-type filter. Defaults to False.
        """
        self.filter_string = filter_string
        self.is_contains = is_contains

    @staticmethod
    def format_value(value: Any) -> str:
        """Format value to string.

        Handles conversion of string type and datetime to OData expected string

        Args:
            value (Any): value to format

        Returns:
            str: value string
        """
        if isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, datetime):
            return make_datetime_utc(value).isoformat().replace("+00:00", "Z")
        return str(value)

    @classmethod
    def build_filter(
        cls, pattern: str, field: str, value: Any, is_contains: bool = False
    ) -> Self:
        """Build a filter from a pattern, field, and value.

        Args:
            pattern (str): pattern to format filter string
            field (str): field name
            value (Any): filter value
            is_contains (bool, optional): is a contains-type filter. Defaults to False.

        Returns:
            Filter: built filter
        """
        return cls(
            pattern.format(field=field, value=cls.format_value(value)), is_contains
        )

    @classmethod
    def eq(cls, field: str, value: Any) -> Self:
        """Equals filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: equals filter
        """
        return cls.build_filter("{field} eq {value}", field, value)

    @classmethod
    def neq(cls, field: str, value: Any) -> Filter:
        """Not equals filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: not equals filter
        """
        return cls.eq(field, value).not_()

    @classmethod
    def contains(cls, field: str, value: Any) -> Self:
        """Contains filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: contains filter
        """
        return cls.build_filter(
            "contains({field},{value})", field, value, is_contains=True
        )

    @classmethod
    def startswith(cls, field: str, value: Any) -> Self:
        """Starts with filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: contains filter
        """
        return cls.build_filter(
            "startswith({field},{value})", field, value, is_contains=True
        )

    @classmethod
    def endswith(cls, field: str, value: Any) -> Self:
        """Ends with filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: contains filter
        """
        return cls.build_filter(
            "endswith({field},{value})", field, value, is_contains=True
        )

    @classmethod
    def gt(cls, field: str, value: Any) -> Self:
        """Greater than filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: greater than filter
        """
        return cls.build_filter("{field} gt {value}", field, value)

    @classmethod
    def lt(cls, field: str, value: Any) -> Self:
        """Less than filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: less than filter
        """
        return cls.build_filter("{field} lt {value}", field, value)

    @classmethod
    def gte(cls, field: str, value: Any) -> Self:
        """Greater than or equal to filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: greater than or equal to filter
        """
        return cls.build_filter("{field} ge {value}", field, value)

    @classmethod
    def lte(cls, field: str, value: Any) -> Self:
        """Less than or equal to filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: less than or equal to filter
        """
        return cls.build_filter("{field} le {value}", field, value)

    @classmethod
    def and_(cls, filters: Iterable[Self]) -> Self:
        """And filter.

        Args:
            filters (Filter): filters to "and"

        Returns:
            Filter: and filter
        """
        filter_string = " and ".join([f.filter_string for f in filters])
        return cls(filter_string)

    @classmethod
    def or_(cls, filters: Iterable[Self]) -> Self:
        """Or filter.

        Args:
            filters (Filter): filters to "or"

        Returns:
            Filter: or filter
        """
        filter_string = " or ".join([f.filter_string for f in filters])
        filter_string = f"({filter_string})"
        return cls(filter_string)

    def not_(self) -> Filter:
        """Not filter.

        Returns:
            Filter: not filer
        """
        if self.is_contains:
            pattern = "not {filter_string}"
        else:
            pattern = "not ({filter_string})"
        return Filter(pattern.format(filter_string=self.filter_string))

    def __str__(self) -> str:
        """Get as string.

        Returns:
            str: string representation
        """
        return self.filter_string

    def __repr__(self) -> str:
        """Get as repr.

        Returns:
            str: repr representation
        """
        return f"Filter<{self.filter_string}>"


class AttributeFilter(Filter):
    """Atrribute Specific Filter."""

    @classmethod
    def build_filter(
        cls,
        pattern: str,
        field: str,
        value: Any,
        is_contains: bool = False,
    ) -> "AttributeFilter":
        """Build an attribute filter from a pattern, field, and value.

        Args:
            pattern (str): pattern to format filter string
            field (str): field name
            value (Any): filter value
            is_contains (bool, optional): is a contains-type filter. Defaults to False.

        Raises:
            ValueError: invalid value

        Returns:
            AttributeFilter: built filter
        """
        if isinstance(value, str):
            value_type = "StringAttribute"
        elif isinstance(value, int):
            value_type = "IntegerAttribute"
        elif isinstance(value, float):
            value_type = "DoubleAttribute"
        elif isinstance(value, datetime):
            value_type = "DateTimeOffsetAttribute"
        else:
            raise ValueError("invalid value type")

        field = f"att:att/Name eq '{field}'"
        attr_field = f"att/OData.CSC.{value_type}/Value"
        value = pattern.format(field=attr_field, value=cls.format_value(value))
        final = f"Attributes/OData.CSC.{value_type}/any({field} and {value})"

        return cls(final, is_contains)
