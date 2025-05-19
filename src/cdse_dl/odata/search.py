"""Search OData Endpoint."""

import itertools
import logging
import urllib.parse
import warnings
from abc import ABC
from collections.abc import Generator
from copy import deepcopy
from datetime import datetime
from typing import Any, Literal

import requests
import shapely.wkt
from shapely.geometry import MultiPolygon, shape
from shapely.geometry.base import BaseGeometry

from cdse_dl.odata.constants import ODATA_BASE_URL
from cdse_dl.odata.filter import AttributeFilter, Filter
from cdse_dl.odata.utils import handle_response
from cdse_dl.types import DatetimeLike, GeometryLike
from cdse_dl.utils import parse_datetime_to_components

AREA_PATTERN = "OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
DELETION_CAUSES = [
    "Duplicated product",
    "Missing checksum",
    "Corrupted product",
    "Obsolete product or Other",
]

logger = logging.getLogger(__name__)


class SearchBase(ABC):
    """CDSE OData endpoint."""

    base_url: str = ""
    expand_options: list[str] = []
    order_by_options: list[str] = []
    order_options: list[str] = ["asc", "desc"]
    select_options: list[str] = []

    def __init__(
        self,
        filter_string: str | None = None,
        skip: int | None = None,
        top: int | None = None,
        order_by: str | None = None,
        order: Literal["asc", "desc"] | None = None,
        expand: str | None = None,
        select: list[str] | None = None,
    ):
        """Search OData endpoint.

        Args:
            filter_string (str | None, optional): filter string to search. Defaults to None.
            skip (int | None], optional): number of entries to skip. Defaults to None.
            top (int | None], optional): number of entries to return. Defaults to None.
            order_by (str | None, optional): order entry by property. Defaults to None.
            order (Literal["asc", "desc"] | None, optional): order of entries. Defaults to None.
            expand (str | None, optional): how to expand entry. Defaults to None.
            filters (list[Filter] | None, optional): extra filters to apply. Defaults to None.
            select (list[str] | None, optional): limit the requested properties to a specific subset. Defaults to None.
        """
        self._parameters = {
            "filter": filter_string,
            "skip": skip,
            "top": top,
            "expand": expand,
            "orderby": _format_order_by(order_by, order),
            "select": select,
        }

    @staticmethod
    def _get(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            response = requests.get(url, params=params)
            handle_response(response)
            content = response.json()
        except Exception as e:
            raise e
        return dict(content)

    def _get_formatted_params(self, count: bool = False) -> dict[str, Any]:
        params = deepcopy(self._parameters)
        params["count"] = str(count) if count else None
        params = {f"${k}": v for k, v in params.items() if v is not None}
        return params

    def pages(self) -> Generator[list[dict[str, Any]]]:
        """Get pages of products.

        Yields:
            Generator[list[dict[str, Any]]]: product pages
        """
        params = urllib.parse.urlencode(self._get_formatted_params())
        next_link = f"{self.base_url}?{params}"

        while next_link:
            content = self._get(next_link)
            page_results = content["value"]
            next_link = content.get("@odata.nextLink")
            yield page_results

    def get(self, limit: int | None = None) -> Generator[dict[str, Any]]:
        """Get products, up to a limit if given.

        Args:
            limit (int | None], optional): optional limit to return. Defaults to None.

        Yields:
            Generator[dict[str, Any]]: products
        """
        self._parameters["top"] = min(
            max(self._parameters.get("top") or 0, limit or 20), 1000
        )
        yield from itertools.islice(itertools.chain.from_iterable(self.pages()), limit)

    def get_all(self) -> list[dict[str, Any]]:
        """Get all products as a list.

        Returns:
            list[dict]: products
        """
        return list(self.get())

    def hits(self) -> int:
        """Get total number of products matching search.

        Returns:
            int: matching hits
        """
        params = self._get_formatted_params(limit=1, count=True)
        content = self._get(self.base_url, params=params)
        return int(content["@odata.count"])


class ProductSearch(SearchBase):
    """Search products on OData endpoint."""

    base_url = ODATA_BASE_URL + "/Products"
    order_by_options = [
        "ContentDate/Start",
        "ContentDate/End",
        "PublicationDate",
        "ModificationDate",
    ]
    expand_options = ["Assets", "Attributes", "Locations"]
    select_options: list[str] = [
        "Id",
        "Name",
        "ContentType",
        "ContentLength",
        "OriginDate",
        "PublicationDate",
        "ModificationDate",
        "Online",
        "EvictionDate",
        "S3Path",
        "Checksum",
        "ContentDate",
        "Footprint",
        "Geofootprint",
        "*",
    ]

    def __init__(
        self,
        collection: str | None = None,
        name: str | None = None,
        product_id: str | None = None,
        date: DatetimeLike | None = None,
        publication_date: DatetimeLike | None = None,
        area: GeometryLike | None = None,
        skip: int | None = None,
        top: int | None = None,
        order_by: str | None = None,
        order: Literal["asc", "desc"] | None = None,
        expand: str | None = None,
        select: list[str] | None = None,
        filters: list[Filter] | None = None,
    ):
        """Search OData endpoint for products.

        Args:
            collection (str | None, optional): collection name to search. Defaults to None.
            name (str | None, optional): product name to search. Defaults to None.
            product_id (str | None, optional): product id to search. Defaults to None.
            date (DatetimeLike | None, optional): sensing datetime / range to search. Defaults to None.
            publication_date (DatetimeLike | None, optional): publication date / range to search. Defaults to None.
            area (GeometryLike | None, optional): area to search. Defaults to None.
            skip (int | None, optional): products to skip. Defaults to None.
            top (int | None, optional): products to return per query. Defaults to None.
            order_by (str | None, optional): order by attribute. Defaults to None.
            order (Literal["asc", "desc"] | None, optional): order direction. Defaults to None.
            expand (str | None, optional): expand products with more detail. Defaults to None.
            select (list[str] | None), optional): fields to select in return response. Defaults to None.
            filters (list[Filter] | None, optional): extra filters to use. Defaults to None.
        """
        filter = build_filter_string(
            collection=collection,
            name=name,
            product_id=product_id,
            date=date,
            publication_date=publication_date,
            area=area,
            extra_filters=filters,
        )
        super().__init__(filter, skip, top, order_by, order, expand, select)


class DeletedProductSearch(SearchBase):
    """Search deleted products on OData endpoint."""

    base_url = ODATA_BASE_URL + "/DeletedProducts"
    order_by_options = [
        "ContentDate/Start",
        "ContentDate/End",
        "DeletionDate",
    ]
    expand_options = ["Attributes"]
    select_options: list[str] = [
        "Id",
        "Name",
        "ContentType",
        "ContentLength",
        "OriginDate",
        "DeletionDate",
        "DeletionCause",
        "Checksum",
        "ContentDate",
        "Footprint",
        "Geofootprint",
        "*",
    ]

    def __init__(
        self,
        name: str | None = None,
        product_id: str | None = None,
        collection: str | None = None,
        date: DatetimeLike | None = None,
        deletion_date: DatetimeLike | None = None,
        origin_date: DatetimeLike | None = None,
        deletion_cause: str | None = None,
        area: GeometryLike | None = None,
        skip: int | None = None,
        top: int | None = None,
        order_by: str | None = None,
        order: Literal["asc", "desc"] | None = None,
        expand: str | None = None,
        select: list[str] | None = None,
        filters: list[Filter] | None = None,
    ):
        """Search OData endpoint for deleted products.

        Args:
            name (str | None, optional): product name to search. Defaults to None.
            product_id (str | None, optional): product id to search. Defaults to None.
            collection (str | None, optional): collection name to search. Defaults to None.
            date (DatetimeLike | None, optional): sensing datetime / range to search. Defaults to None.
            deletion_date (DatetimeLike | None, optional): deletion datetime / range to search. Defaults to None.
            origin_date (DatetimeLike | None, optional): origin datetime/ range to search. Defaults to None.
            deletion_cause (str | None, optional): deletion cause. Defaults to None.
            area (GeometryLike | None, optional): area to search. Defaults to None.
            skip (int | None], optional): products to skip. Defaults to None.
            top (int | None], optional): products to return per query. Defaults to None.
            order_by (str | None, optional): order by attribute. Defaults to None.
            order (Literal["asc", "desc"] | None, optional): order direction. Defaults to None.
            expand (str | None, optional): expand products with more detail. Defaults to None.
            select (list[str] | None), optional): fields to select in return response. Defaults to None.
            filters (list[Filter] | None, optional): extra filters to use. Defaults to None.
        """
        filter = build_filter_string(
            collection=collection,
            name=name,
            product_id=product_id,
            date=date,
            deletion_date=deletion_date,
            origin_date=origin_date,
            deletion_cause=deletion_cause,
            area=area,
            extra_filters=filters,
        )
        super().__init__(filter, skip, top, order_by, order, expand, select)


def _format_order_by(order_by: str | None, order: str | None) -> str | None:
    """Format order by with order direction.

    If order_by is not given, None is returned

    Args:
        order_by (str | None, optional): order by field
        order (str | None, optional): order direction

    Returns:
        str | None: formatted string or `None`
    """
    if not order_by:
        if order:
            warnings.warn("`order` provided without `order_by`; ignoring order...")
        return None

    return f"{order_by} {order}" if order else order_by


def _filter_from_datetime_components(
    components: list[datetime | None], field: str
) -> Filter:
    """Build Filter from datetime components.

    Args:
        components (list[datetime] | None): datetime component
        field (str): field to build filter on

    Returns:
        Filter: datetime filter
    """
    filters = []

    if components[0] is not None:
        filters.append(Filter.gte(field, components[0]))
    if components[1] is not None:
        filters.append(Filter.lt(field, components[1]))

    # AND the filters if we have both dates
    if len(filters) == 1:
        return filters[0]
    else:
        return Filter.and_(filters)


def build_datetime_filter(value: DatetimeLike, field: str) -> Filter:
    """Construct a datetime filter from a datetime or datetime list.

    If string, will break on / to construct datetime range

    If None on one side or other, the filter will be open-ended

    Args:
        value (DatetimeLike): datetime or datetime list
        field (str): field to construct filter on

    Returns:
        Filter: datetime filter
    """
    components = parse_datetime_to_components(value)
    return _filter_from_datetime_components(components, field)


def build_area_filter(value: GeometryLike) -> Filter:
    """Build area filter.

    Args:
        value (GeometryLike): geometry to filter

    Raises:
        ValueError: cannot parse value

    Returns:
        Filter: area filter
    """
    if isinstance(value, str):
        try:
            value = shapely.wkt.loads(value)
        except Exception:
            raise ValueError("Could not parse str from wkt to geometry")

        return build_area_filter(value)

    elif isinstance(value, dict):
        try:
            value = shape(value)
        except Exception:
            raise ValueError("Could not parse dict to geometry")
        return build_area_filter(value)

    elif isinstance(value, BaseGeometry):
        if isinstance(value, MultiPolygon):
            raise ValueError("multipolygon not supported")
        else:
            wkt = shapely.wkt.dumps(value)
            return Filter(AREA_PATTERN.format(wkt=wkt))
    else:
        raise ValueError(f"Invalid value type: {type(value)}")


def build_filter_string(
    *,
    collection: str | None = None,
    name: str | None = None,
    product_id: str | None = None,
    date: DatetimeLike | None = None,
    publication_date: DatetimeLike | None = None,
    deletion_date: DatetimeLike | None = None,
    origin_date: DatetimeLike | None = None,
    deletion_cause: str | None = None,
    area: GeometryLike | None = None,
    extra_filters: list[Filter] | None = None,
) -> str | None:
    """Build filter string.

    Args:
        collection (str | None, optional): collection name. Defaults to None.
        name (str | None, optional): product name. Defaults to None.
        product_id (str | None, optional): product id. Defaults to None.
        date (DatetimeLike | None, optional): sensing datetime filter. Defaults to None.
        publication_date (DatetimeLike | None, optional): publication datetime filter. Defaults to None.
        deletion_date (DatetimeLike | None, optional): deletion datetime filter. Defaults to None.
        origin_date (DatetimeLike | None, optional): origin datetime filter. Defaults to None.
        deletion_cause (str | None, optional): deletion cause. Defaults to None.
        area (GeometryLike | None, optional): area value. Defaults to None.
        extra_filters (list[Filter] | None, optional): extra custom filters. Defaults to None.

    Returns:
        str: filter string
    """
    filters: list[Filter | AttributeFilter] = []
    if collection:
        filters.append(Filter.eq("Collection/Name", collection))
    if name:
        if name.startswith("*") or name.endswith("*"):
            filters.append(Filter.contains("Name", name.replace("*", "")))
        else:
            filters.append(Filter.eq("Name", name))
    if product_id:
        filters.append(Filter.eq("Id", product_id))
    if date:
        filters.append(build_datetime_filter(date, "ContentDate/Start"))
    if publication_date:
        filters.append(build_datetime_filter(publication_date, "PublicationDate"))
    if deletion_date:
        filters.append(build_datetime_filter(deletion_date, "DeletionDate"))
    if origin_date:
        filters.append(build_datetime_filter(origin_date, "OriginDate"))
    if deletion_cause:
        filters.append(Filter.eq("DeletionCause", deletion_cause))
    if area:
        filters.append(build_area_filter(area))
    if extra_filters:
        filters += extra_filters

    logger.debug(f"Using Filters: {filters}")

    if len(filters) == 0:
        return None
    if len(filters) == 1:
        return str(filters[0])
    else:
        return str(Filter.and_(filters))
