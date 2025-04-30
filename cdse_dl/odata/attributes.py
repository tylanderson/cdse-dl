"""OData Attributes."""

from functools import lru_cache
from typing import Any, Dict, Optional, Tuple

import requests

ATTRIBUTES_ENDPOINT = "https://catalogue.dataspace.copernicus.eu/odata/v1/Attributes"


@lru_cache(1)
def get_attribute_info() -> Dict[str, Any]:
    """Get attribute info from OData.

    Returns:
        Dict: attribute info
    """
    return dict(requests.get(ATTRIBUTES_ENDPOINT).json())


@lru_cache()
def get_attribute_type(collection: str, attribute_name: str) -> Optional[str]:
    """Get attribute type from OData from collection and attribute name.

    Args:
        collection (str): collection name
        attribute_name (str): attribute name

    Returns:
        Optional[str]: attribute type
    """
    for attr in get_attribute_info()[collection]:
        if attribute_name == attr["Name"]:
            return str(attr["ValueType"])

    return None


def get_collections() -> Tuple[str, ...]:
    """Get all known collections.

    Returns:
        Tuple[str, ...]: known collections
    """
    return tuple(get_attribute_info().keys())


def get_collection_attributes(collection: str) -> Tuple[str, ...]:
    """Get all know attributes that can be queried for a collection.

    Args:
        collection (str): collection name

    Raises:
        ValueError: invalid collection

    Returns:
        Tuple[str, ...]: known attributes

    """
    if collection not in get_collections():
        raise ValueError(f"Invalid Collection: {collection}")
    return tuple(attr["Name"] for attr in get_attribute_info()[collection])
