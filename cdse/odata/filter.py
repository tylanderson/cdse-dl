"""OData Filter creation and helpers."""

from datetime import datetime
from typing import Any

import pytz


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
        return dt.replace(tzinfo=pytz.utc)
    else:
        # It's already timezone-aware, return as is
        return dt.astimezone(pytz.utc)


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
        # Add more types as necessary
        return str(value)

    @classmethod
    def build_filter(cls, pattern: str, field: str, value: Any, **kwargs) -> "Filter":
        """Build a filter from a pattern, field, and value.

        Args:
            pattern (str): pattern to format filter string
            field (str): field name
            value (Any): filter value
            **kwargs: optional kwargs to pass down to constructor

        Returns:
            Filter: built filter
        """
        return cls(pattern.format(field=field, value=cls.format_value(value)), **kwargs)

    @classmethod
    def eq(cls, field: str, value: Any) -> "Filter":
        """Equals filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: equals filter
        """
        return cls.build_filter("{field} eq {value}", field, value)

    @classmethod
    def neq(cls, field: str, value: Any) -> "Filter":
        """Not equals filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: not equals filter
        """
        return cls.eq(field, value).not_()

    @classmethod
    def contains(cls, field: str, value: Any) -> "Filter":
        """Contains filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: contains filter
        """
        if not isinstance(value, str):
            raise ValueError("Value must be a string")
        return cls.build_filter(
            "contains({field},{value})", field, value, is_contains=True
        )

    @classmethod
    def startswith(cls, field: str, value: Any) -> "Filter":
        """Starts with filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: contains filter
        """
        if not isinstance(value, str):
            raise ValueError("Value must be a string")
        return cls.build_filter(
            "startswith({field},{value})", field, value, is_contains=True
        )

    @classmethod
    def endswith(cls, field: str, value: Any) -> "Filter":
        """Ends withs filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: contains filter
        """
        if not isinstance(value, str):
            raise ValueError("Value must be a string")
        return cls.build_filter(
            "endswith({field},{value})", field, value, is_contains=True
        )

    @classmethod
    def gt(cls, field: str, value: Any) -> "Filter":
        """Greater than filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: greater than filter
        """
        return cls.build_filter("{field} gt {value}", field, value)

    @classmethod
    def lt(cls, field: str, value: Any) -> "Filter":
        """Less than filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: less than filter
        """
        return cls.build_filter("{field} lt {value}", field, value)

    @classmethod
    def gte(cls, field: str, value: Any) -> "Filter":
        """Greater than or equal to filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: greater than or equal to filter
        """
        return cls.build_filter("{field} ge {value}", field, value)

    @classmethod
    def lte(cls, field: str, value: Any) -> "Filter":
        """Less than or equal to filter.

        Args:
            field (str): field name
            value (Any): value

        Returns:
            Filter: less than or equal to filter
        """
        return cls.build_filter("{field} le {value}", field, value)

    def and_(self, other: "Filter") -> "Filter":
        """And filter.

        Args:
            other (Filter): filter to "and"

        Returns:
            Filter: and filter
        """
        return Filter(f"{self.filter_string} and {other.filter_string}")

    def or_(self, other: "Filter") -> "Filter":
        """Or filter.

        Args:
            other (Filter): filter to "or"

        Returns:
            Filter: or filter
        """
        return Filter(f"({self.filter_string} or {other.filter_string})")

    def not_(self) -> "Filter":
        """Not filter.

        Returns:
            Filter: not filer
        """
        if self.is_contains:
            pattern = "not {filter_string}"
        else:
            pattern = "not ({filter_string})"
        return Filter(pattern.format(filter_string=self.filter_string))

    def __str__(self):
        """Get as string."""
        return self.filter_string

    def __repr__(self):
        """Get as repr."""
        return f"Filter<{self.filter_string}>"


class AttributeFilter(Filter):
    """Atrribute Specific Filter."""

    @classmethod
    def build_filter(
        cls, pattern: str, field: str, value: Any, **kwargs
    ) -> "AttributeFilter":
        """Build an attribute filter from a pattern, field, and value.

        Args:
            pattern (str): pattern to format filter string
            field (str): field name
            value (Any): filter value
            **kwargs: optional kwargs to pass down to constructor

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

        return cls(final, **kwargs)
