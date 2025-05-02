"""OData Attributes."""

from functools import lru_cache
from typing import Literal, TypedDict

import requests

from cdse_dl.odata.constants import ODATA_BASE_URL

ATTRIBUTES_ENDPOINT = f"{ODATA_BASE_URL}/Attributes"


class Attribute(TypedDict):
    """attribute of a product."""

    Name: str
    ValueType: Literal["String", "Integer", "Double", "DateTimeOffset", "Boolean"]


CollectionAttributes = dict[str, list[Attribute]]


@lru_cache(maxsize=1)
def get_attribute_info() -> CollectionAttributes:
    """Get attribute info from OData.

    Returns:
        ProductAttributes: attribute info
    """
    response = requests.get(ATTRIBUTES_ENDPOINT)
    response.raise_for_status()
    product_attributes: CollectionAttributes = response.json()
    return product_attributes


@lru_cache
def get_attribute_type(collection: str, attribute_name: str) -> str | None:
    """Get attribute type from OData from collection and attribute name.

    Args:
        collection (str): collection name
        attribute_name (str): attribute name

    Returns:
        str | None: attribute type
    """
    for attr in get_attribute_info()[collection]:
        if attribute_name == attr["Name"]:
            return str(attr["ValueType"])

    return None


def get_collections() -> tuple[str, ...]:
    """Get all known collections.

    Returns:
        tuple[str, ...]: known collections
    """
    return tuple(get_attribute_info().keys())


def get_collection_attributes(collection: str) -> tuple[str, ...]:
    """Get all know attributes that can be queried for a collection.

    Args:
        collection (str): collection name

    Raises:
        ValueError: invalid collection

    Returns:
        tuple[str, ...]: known attributes

    """
    collections = get_collections()
    if collection not in collections:
        raise ValueError(
            f"Invalid collection: {collection}. Available: {sorted(collections)}"
        )

    attributes = get_attribute_info()["collection"]
    return tuple(attr["Name"] for attr in attributes)
