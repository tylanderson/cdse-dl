"""Module for interacting with the Copernicus Data Space Environment (CDSE) Trace API."""

from typing import Any

import requests

TRACE_BASE_URL = "https://trace.dataspace.copernicus.eu/api/v1/traces"


def obsolete(
    name_prefix: str | None = None,
    start: str | None = None,
    end: str | None = None,
    after: str | None = None,
) -> Any:
    """Get obsolete traces.

    Args:
        name_prefix (str | None, optional): filter obsolete traces by name prefix. Defaults to None.
        start (str | None, optional): filter obsolete traces created after this date. ISO 8601 format. Defaults to None.
        end (str | None, optional): filter obsolete traces created before this date. ISO 8601 format. Defaults to None.
        after (str | None, optional): filter obsolete traces created after this date. ISO 8601 format. Defaults to None.

    Returns:
        Any: search results
    """
    params = {}
    if name_prefix:
        params["name_prefix"] = name_prefix
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if after:
        params["after"] = after

    r = requests.get(f"{TRACE_BASE_URL}/obsolete", params=params)
    r.raise_for_status()
    return r.json()


def from_id(id: str) -> Any:
    """Get trace by id.

    Args:
        id (str): trace id

    Returns:
        dict[str, Any]: trace info
    """
    r = requests.get(f"{TRACE_BASE_URL}/{id}")
    r.raise_for_status()
    return r.json()


def from_name(product_name: str) -> Any:
    """Get trace by product name.

    Args:
        product_name (str): product name

    Returns:
        Any: trace info
    """
    r = requests.get(f"{TRACE_BASE_URL}/name/{product_name}")
    r.raise_for_status()
    return r.json()


def from_hash(hash: str) -> Any:
    """Get trace by hash.

    Args:
        hash (str): trace hash

    Returns:
        Any: trace info
    """
    r = requests.get(f"{TRACE_BASE_URL}/hash/{hash}")
    r.raise_for_status()
    return r.json()


def validate(product_name: str, hash: str) -> bool:
    """Validate a product against the trace.

    Args:
        product_name (str): product name
        hash (str): trace hash

    Returns:
        bool: whether the product is valid according to the trace
    """
    r = requests.get(
        f"{TRACE_BASE_URL}/validate",
        params={"product_name": product_name, "hash": hash},
    )
    r.raise_for_status()
    valid: bool = r.json().get("success", False)
    return valid
