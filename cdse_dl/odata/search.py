"""Search OData Endpoint."""

import logging
from abc import ABC
from copy import deepcopy
from datetime import datetime
from typing import Any, Literal, Optional, Union

import requests
import shapely.wkt
from shapely.geometry import MultiPolygon, shape
from shapely.geometry.base import BaseGeometry

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


class SearchBase(ABC):
    """CDSE OData endpoint."""

    base_url: str = ""
    expand_options: list[str] = []
    order_by_options: list[str] = []
    order_options: list[str] = ["asc", "desc"]
    select_options: list[str] = []

    def __init__(
        self,
        filter_string: Optional[str] = None,
        skip: Optional[int] = None,
        top: Optional[int] = None,
        order_by: Optional[str] = None,
        order: Optional[Literal["asc", "desc"]] = None,
        expand: Optional[str] = None,
        select: Optional[list[str]] = None,
    ):
        """Search OData endpoint.

        Args:
            filter_string (Optional[str], optional): filter string to search. Defaults to None.
            skip (Optional[int], optional): number of entries to skip. Defaults to None.
            top (Optional[int], optional): number of entries to return. Defaults to None.
            order_by (Optional[str], optional): order entry by property. Defaults to None.
            order (Optional[Literal["asc", "desc"]], optional): order of entries. Defaults to None.
            expand (Optional[str], optional): how to expand entry. Defaults to None.
            filters (Optional[list[Filter]], optional): extra filters to apply. Defaults to None.
            select (Optional[list[str]], optional): limit the requested properties to a specific subset. Defaults to None.
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
    def _get(url: str, params: dict[str, Any]) -> dict[str, Any]:
        try:
            logging.debug(f"GET with params: {params}")
            response = requests.get(url, params=params)
            handle_response(response)
            content = response.json()
        except Exception as e:
            raise e
        return dict(content)

    def _get_formatted_params(self, limit: int, count: bool = False) -> dict[str, Any]:
        params = deepcopy(self._parameters)
        if limit and not params.get("top"):
            params["top"] = limit
        params["count"] = str(count) if count else None
        params = {f"${k}": v for k, v in params.items() if v is not None}
        return params

    def get(self, limit: int = 1000) -> list[dict[str, Any]]:
        """Get products, up to a limit if given.

        Args:
            limit (Optional[int], optional): optional limit to return. Defaults to 1000.

        Returns:
            list[dict]: products
        """
        results = []
        more_results = True
        count = 0

        url = self.base_url
        params = self._get_formatted_params(limit)

        while more_results:
            content = self._get(url, params)
            page_results = content["value"]
            results.extend(page_results)
            count += len(page_results)

            next_link = content.get("@odata.nextLink")
            if next_link is None or (limit is not None and count >= limit):
                more_results = False
            else:
                url = next_link
                params = {}

        return results

    def get_all(self) -> list[dict[str, Any]]:
        """Get all products.

        Returns:
            list[dict]: products
        """
        return self.get()

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

    base_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
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
        collection: Optional[str] = None,
        name: Optional[str] = None,
        product_id: Optional[str] = None,
        date: Optional[DatetimeLike] = None,
        publication_date: Optional[DatetimeLike] = None,
        area: Optional[GeometryLike] = None,
        skip: Optional[int] = None,
        top: Optional[int] = None,
        order_by: Optional[str] = None,
        order: Optional[Literal["asc", "desc"]] = None,
        expand: Optional[str] = None,
        select: Optional[list[str]] = None,
        filters: Optional[list[Filter]] = None,
    ):
        """Search OData endpoint for products.

        Args:
            collection (Optional[str], optional): collection name to search. Defaults to None.
            name (Optional[str], optional): product name to search. Defaults to None.
            product_id (Optional[str], optional): product id to search. Defaults to None.
            date (Optional[DatetimeLike], optional): sensing datetime / range to search. Defaults to None.
            publication_date (Optional[DatetimeLike], optional): publication date / range to search. Defaults to None.
            area (Optional[GeometryLike], optional): area to search. Defaults to None.
            skip (Optional[int, optional): products to skip. Defaults to None.
            top (Optional[int, optional): products to return per query. Defaults to None.
            order_by (Optional[str], optional): order by attribute. Defaults to None.
            order (Optional[Literal["asc", "desc"]], optional): order direction. Defaults to None.
            expand (Optional[str], optional): expand products with more detail. Defaults to None.
            select (Optional[list[str]]), optional): fields to select in return response. Defaults to None.
            filters (Optional[list[Filter]], optional): extra filters to use. Defaults to None.
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

    base_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/DeletedProducts"
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
        name: Optional[str] = None,
        product_id: Optional[str] = None,
        collection: Optional[str] = None,
        date: Optional[DatetimeLike] = None,
        deletion_date: Optional[DatetimeLike] = None,
        origin_date: Optional[DatetimeLike] = None,
        deletion_cause: Optional[str] = None,
        area: Optional[GeometryLike] = None,
        skip: Optional[int] = None,
        top: Optional[int] = None,
        order_by: Optional[str] = None,
        order: Optional[Literal["asc", "desc"]] = None,
        expand: Optional[str] = None,
        select: Optional[list[str]] = None,
        filters: Optional[list[Filter]] = None,
    ):
        """Search OData endpoint for deleted products.

        Args:
            name (Optional[str], optional): product name to search. Defaults to None.
            product_id (Optional[str], optional): product id to search. Defaults to None.
            collection (Optional[str], optional): collection name to search. Defaults to None.
            date (Optional[DatetimeLike], optional): sensing datetime / range to search. Defaults to None.
            deletion_date (Optional[DatetimeLike], optional): deletion datetime / range to search. Defaults to None.
            origin_date (Optional[DatetimeLike], optional): origin datetime/ range to search. Defaults to None.
            deletion_cause (Optional[str], optional): deletion cause. Defaults to None.
            area (Optional[GeometryLike], optional): area to search. Defaults to None.
            skip (Optional[int], optional): products to skip. Defaults to None.
            top (Optional[int], optional): products to return per query. Defaults to None.
            order_by (Optional[str], optional): order by attribute. Defaults to None.
            order (Optional[Literal["asc", "desc"]], optional): order direction. Defaults to None.
            expand (Optional[str], optional): expand products with more detail. Defaults to None.
            select (Optional[list[str]]), optional): fields to select in return response. Defaults to None.
            filters (Optional[list[Filter]], optional): extra filters to use. Defaults to None.
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


def _format_order_by(order_by: Optional[str], order: Optional[str]) -> Optional[str]:
    """Format order by with order direction.

    If order_by is not given, None is returned

    Args:
        order_by (Optional[str], optional): order by field
        order (Optional[str], optional): order direction

    Returns:
        Optional[str]: formatted string or `None`
    """
    if order_by:
        if order:
            return f"{order_by} {order}"
        else:
            return order_by
    else:
        return None


def _filter_from_datetime_components(
    components: list[Optional[datetime]], field: str
) -> Filter:
    """Build Filter from datetime components.

    Args:
        components (Optional[list[datetime]]): datetime component
        field (str): field to build filter on

    Returns:
        Optional[Filter]: datetime filter
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
        Optional[Filter]: datetime filter
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
    collection: Optional[str] = None,
    name: Optional[str] = None,
    product_id: Optional[str] = None,
    date: Optional[DatetimeLike] = None,
    publication_date: Optional[DatetimeLike] = None,
    deletion_date: Optional[DatetimeLike] = None,
    origin_date: Optional[DatetimeLike] = None,
    deletion_cause: Optional[str] = None,
    area: Optional[GeometryLike] = None,
    extra_filters: Optional[list[Filter]] = None,
) -> Optional[str]:
    """Build filter string.

    Args:
        collection (Optional[str], optional): collection name. Defaults to None.
        name (Optional[str], optional): product name. Defaults to None.
        product_id (Optional[str], optional): product id. Defaults to None.
        date (Optional[DatetimeLike], optional): sensing datetime filter. Defaults to None.
        publication_date (Optional[DatetimeLike], optional): publication datetime filter. Defaults to None.
        deletion_date (Optional[DatetimeLike], optional): deletion datetime filter. Defaults to None.
        origin_date (Optional[DatetimeLike], optional): origin datetime filter. Defaults to None.
        deletion_cause (Optional[str], optional): deletion cause. Defaults to None.
        area (Optional[GeometryLike], optional): area value. Defaults to None.
        extra_filters (Optional[list[Filter]], optional): extra custom filters. Defaults to None.

    Returns:
        str: filter string
    """
    filters: list[Union[Filter, AttributeFilter]] = []
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

    logging.debug(f"Using Filters: {filters}")

    if len(filters) == 0:
        return None
    if len(filters) == 1:
        return str(filters[0])
    else:
        return str(Filter.and_(filters))
