"""Search OData Endpoint."""

import logging
from abc import ABC
from copy import deepcopy
from datetime import datetime
from functools import reduce
from typing import Dict, List, Literal, Optional, Sequence, Union

import requests
import shapely.wkt
from dateutil.parser import parse as dt_parse
from shapely.geometry import MultiPolygon, shape
from shapely.geometry.base import BaseGeometry

from ..types import DatetimeLike, GeometryLike
from .filter import AttributeFilter, Filter, make_datetime_utc

AREA_PATTERN = "OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
DELETION_CAUSES = [
    "Duplicated product",
    "Missing checksum",
    "Corrupted product",
    "Obsolete product or Other",
]


def handle_response(response: requests.Response) -> None:
    """Check response for errors.

    Args:
        response (requests.Response): response

    Raises:
        Exception: Invalid Request
    """
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        try:
            response_detail = response.json()["detail"]
        except Exception:
            response_detail = {"message": response.text, "request_id": "N/A"}
        raise Exception(
            f"Request Failed: {response_detail['message']} (Request ID: {response_detail['request_id']})"
        ) from e


class SearchBase(ABC):
    """CDSE OData endpoint."""

    base_url: str = ""
    expand_options: List[str] = []
    order_by_options: List[str] = []
    order_options: List[str] = ["asc", "desc"]

    def __init__(
        self,
        filter_string: Optional[str] = None,
        skip: Optional[int] = None,
        top: Optional[int] = 1000,
        order_by: Optional[str] = None,
        order: Optional[Literal["asc", "desc"]] = "asc",
        expand: Optional[str] = None,
    ):
        """Search OData endpoint.

        Args:
            filter_string (Optional[str], optional): filter string to search. Defaults to None.
            skip (Optional[int], optional): number of entries to skip. Defaults to None.
            top (Optional[int], optional): number of entries to return. Defaults to None.
            order_by (Optional[str], optional): order entry by property. Defaults to None.
            order (Optional[Literal["asc", "desc"]], optional): order of entries. Defaults to "asc".
            expand (Optional[str], optional): how to expand entry. Defaults to None.
            filters (Optional[List[Filter]], optional): extra filters to apply. Defaults to None.

        Returns:
            Dict[str, Any]: entries
        """
        if top is not None and (top > 1000 or top < 0):
            raise ValueError("top must be between 0 and 1000")

        if skip is not None and (skip > 10000 or skip < 0):
            raise ValueError("skip must be between 0 and 10000")

        if expand is not None and (expand not in self.expand_options):
            raise ValueError(
                f"Invalid `expend` '{expand}', must be one of {self.expand_options}"
            )
        if order_by is not None and (order_by not in self.order_by_options):
            raise ValueError(
                f"Invalid `order_by` '{order_by}', must be one of {self.order_by_options}"
            )
        if order is not None and (order not in self.order_options):
            raise ValueError(
                f"Invalid `order` '{order}', must be one of {self.order_options}"
            )

        self._parameters = {
            "filter": filter_string,
            "skip": skip,
            "top": top,
            "expand": expand,
            "order_by": _format_order_by(order_by, order),
        }

    @staticmethod
    def _get(url, params):
        try:
            response = requests.get(url, params=params)
            handle_response(response)
            content = response.json()
        except Exception as e:
            raise e
        return content

    def _get_formatted_params(self, limit, count=False):
        params = deepcopy(self._parameters)
        if limit:
            params["top"] = min(params["top"], limit)
        params["count"] = str(count) if count else None
        params = {f"${k}": v for k, v in params.items() if v is not None}
        return params

    def get(self, limit: Optional[int] = 1000) -> List[Dict]:
        """Get products, up to a limit if given.

        Args:
            limit (Optional[int], optional): optional limit to return. Defaults to 1000.

        Returns:
            List[Dict]: products
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

    def get_all(self) -> List[Dict]:
        """Get all products.

        Returns:
            List[Dict]: products
        """
        return self.get(limit=None)

    def hits(self) -> int:
        """Get total number of products matching search.

        Returns:
            int: matching hits
        """
        params = self._get_formatted_params(limit=1, count=True)
        content = self._get(self.base_url, params=params)
        return content["@odata.count"]


class ProductSearch(SearchBase):
    """Search products on OData endpoint."""

    base_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    order_by_options = [
        "ContentDate/Start",
        "ContentDate/End",
        "PublicationDate",
        "ModificationDate",
    ]
    expand_options = ["Assets", "Attributes"]

    def __init__(
        self,
        collection: Optional[str] = None,
        name: Optional[str] = None,
        date: Optional[DatetimeLike] = None,
        publication_date: Optional[DatetimeLike] = None,
        area: Optional[GeometryLike] = None,
        skip: Optional[int] = None,
        top: Optional[int] = 1000,
        order_by: Optional[str] = None,
        order: Optional[Literal["asc", "desc"]] = "asc",
        expand: Optional[str] = None,
        filters: Optional[List[Filter]] = None,
    ):
        """Search OData endpoint for products.

        Args:
            collection (Optional[str], optional): collection name to search. Defaults to None.
            name (Optional[str], optional): product name to search. Defaults to None.
            date (Optional[DatetimeLike], optional): sensing datetime / range to search. Defaults to None.
            publication_date (Optional[DatetimeLike], optional): publication date / range to search. Defaults to None.
            area (Optional[GeometryLike], optional): area to search. Defaults to None.
            skip (Optional[int, optional): products to skip. Defaults to None.
            top (Optional[int, optional): products to return per query. Defaults to 1000.
            order_by (Optional[str], optional): order by attribute. Defaults to None.
            order (Optional[Literal["asc", "desc"]], optional): order direction. Defaults to "asc".
            expand (Optional[str], optional): expand products with more detail. Defaults to None.
            filters (Optional[List[Filter]], optional): extra filters to use. Defaults to None.
        """
        filter = build_filter_string(
            collection=collection,
            name=name,
            date=date,
            publication_date=publication_date,
            area=area,
            extra_filters=filters,
        )
        super().__init__(filter, skip, top, order_by, order, expand)


class DeletedProductSearch(SearchBase):
    """Search deleted products on OData endpoint."""

    base_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/DeletedProducts"
    order_by_options = [
        "ContentDate/Start",
        "ContentDate/End",
        "DeletionDate",
    ]
    expand_options = ["Attributes"]

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
        top: Optional[int] = 1000,
        order_by: Optional[str] = None,
        order: Optional[Literal["asc", "desc"]] = "asc",
        expand: Optional[str] = None,
        filters: Optional[List[Filter]] = None,
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
            top (Optional[int], optional): products to return per query. Defaults to 1000.
            order_by (Optional[str], optional): order by attribute. Defaults to None.
            order (Optional[Literal["asc", "desc"]], optional): order direction. Defaults to "asc".
            expand (Optional[str], optional): expand products with more detail. Defaults to None.
            filters (Optional[List[Filter]], optional): extra filters to use. Defaults to None.
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
        super().__init__(filter, skip, top, order_by, order, expand)


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


def _parse_datetime_to_components(
    value: DatetimeLike,
) -> List[Optional[datetime]]:
    """Convert DatetimeLike to list of datetimes.

    Args:
        value (DatetimeLike): DatetimeLike to convert

    Returns:
        Optional[List[Optional[datetime]]]: datetime components
    """
    components: Sequence[Union[str, datetime]]

    if isinstance(value, datetime):
        return [make_datetime_utc(value)]
    elif isinstance(value, str):
        components = value.split("/")

    datetime_components: List[Optional[datetime]] = []
    for component in components:
        if component:
            if isinstance(component, str):
                component = dt_parse(component)
            datetime_components.append(make_datetime_utc(component))
        else:
            datetime_components.append(None)

    return datetime_components


def _filter_from_datetime_components(
    components: List[Optional[datetime]], field: str
) -> Filter:
    """Build Filter from datetime comomenets.

    Args:
        components (Optional[List[datetime]]): datetime component
        field (str): field to build filter on

    Returns:
        Optional[Filter]: datetime filter
    """
    filters = []
    if len(components) == 1:
        return Filter.gte(field, components[0])
    elif len(components) == 2:
        if all(c is None for c in components):
            raise Exception("cannot create a double open-ended interval")
        if components[0] is not None:
            filters.append(Filter.gte(field, components[0]))
        if components[1] is not None:
            filters.append(Filter.lt(field, components[1]))
        if len(filters) == 1:
            return filters[0]
        else:
            return filters[0].and_(filters[1])
    else:
        raise Exception(
            "too many datetime components "
            f"(max=2, actual={len(components)}): {components}"
        )


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
    components = _parse_datetime_to_components(value)
    return _filter_from_datetime_components(components, field)


def build_area_filter(value: GeometryLike) -> Filter:
    """Build area filter.

    Args:
        value (GeometryLike): geometry to filter

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
    extra_filters: Optional[List[Filter]] = None,
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
        extra_filters (Optional[List[Filter]], optional): extra custom filters. Defaults to None.

    Returns:
        str: filter string
    """
    filters: List[Union[Filter, AttributeFilter]] = []
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

    logging.debug(filters)

    if len(filters) == 0:
        return None
    if len(filters) == 1:
        return str(filters[0])
    else:
        return str(reduce(lambda a, b: a.and_(b), filters))
