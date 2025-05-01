"""Search OpenSearch Endpoint."""

from collections.abc import Generator
from typing import Any

import requests

from cdse_dl.types import DatetimeLike, GeometryLike
from cdse_dl.utils import parse_datetime_to_components

SEARCH_BASE_URL = "https://catalogue.dataspace.copernicus.eu/resto/api"


def _snake_to_camel(snake_str: str) -> str:
    """Convert snakecase string to camelcase.

    Args:
        snake_str (str): snakecase string

    Returns:
        str: camelcase string
    """
    components = snake_str.split("_")
    return components[0] + "".join(x.capitalize() for x in components[1:])


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
            error_msg = response_detail["ErrorMessage"].rstrip(".")
            error_detail = response_detail["ErrorDetail"][0]["msg"]
            request_id = response_detail["RequestID"]
        except Exception:
            response_detail = {"message": response.text, "request_id": "N/A"}
        raise Exception(
            f"{error_msg}: {error_detail} (Request ID: {request_id})"
        ) from e


def format_params(
    name: str | None = None,
    product_id: str | None = None,
    date: DatetimeLike | None = None,
    publication_date: DatetimeLike | None = None,
    geometry: GeometryLike | None = None,
    point: tuple[float, float] | None = None,
    radius: float | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    cloud_cover: tuple[int, int] | None = None,
    instrument: str | None = None,
    product_type: str | None = None,
    orbit_direction: str | None = None,
    resolution: str | None = None,
    sensor_mode: str | None = None,
    status: str | None = None,
    **kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Format params for OpenSearch.

    Args:
        name (str | None, optional): collection name to search. Defaults to None.
        product_id (str | None, optional): product id to search. Defaults to None.
        date (DatetimeLike | None, optional): sensing datetime / range to search. Defaults to None.
        publication_date (DatetimeLike | None, optional): publication date / range to search. Defaults to None.
        geometry (GeometryLike | None, optional): area to search. Defaults to None.
        point (tuple[float, float] | None, optional): point to search. Defaults to None.
        radius (float | None, optional): radius to buffer point by. Defaults to None.
        bbox (tuple[float, float, float, float] | None, optional): bbox to search. Defaults to None.
        cloud_cover (tuple[int, int] | None, optional): cloud cover range to filter on. Defaults to None.
        instrument (str | None, optional): instrument to filter on. Defaults to None.
        product_type (str | None, optional): product type to filter on. Defaults to None.
        orbit_direction (str | None, optional): orbit direction to filter on. Defaults to None.
        resolution (str | None, optional): resolution to filter on. Defaults to None.
        sensor_mode (str | None, optional): sensor mode to filter on. Defaults to None.
        status (str | None, optional): status to filter on. Defaults to None.

    Keyword Arguments:
        **kwargs: keyword arguments to additionally format

    Returns:
        dict: formatted params
    """
    params: dict[str, Any] = {}

    if name:
        params["productIdentifier"] = name
    if product_id:
        params["identifier"] = product_id
    if date:
        start_date, end_date = parse_datetime_to_components(date)
        if start_date:
            params["startDate"] = start_date.isoformat()
        if end_date:
            params["completionDate"] = end_date.isoformat()
    if publication_date:
        start_date, end_date = parse_datetime_to_components(publication_date)
        if start_date:
            params["publishedAfter"] = start_date.isoformat()
        if end_date:
            params["publishedBefore"] = end_date.isoformat()
    if geometry:
        ...
    if point:
        assert len(point) == 2
        params["lon"] = point[0]
        params["lat"] = point[1]
    if radius and point is not None:
        params["radius"] = radius
    if bbox:
        params["box"] = bbox
    if cloud_cover:
        assert len(cloud_cover) == 2
        params["cloudCover"] = f"[{cloud_cover[0]},{cloud_cover[1]}]"
    if instrument:
        params["instrument"] = instrument
    if product_type:
        params["productType"] = product_type
    if sensor_mode:
        params["sensorMode"] = sensor_mode
    if orbit_direction:
        params["orbitDirection"] = orbit_direction
    if resolution:
        params["resolution"] = resolution
    if status:
        params["status"] = status

    for k, v in kwargs.items():
        if k not in params:
            params[_snake_to_camel(k)] = v

    return params


class ProductSearch:
    """Search OpenSearch."""

    def __init__(
        self,
        collection: str | None = None,
        name: str | None = None,
        product_id: str | None = None,
        date: DatetimeLike | None = None,
        publication_date: DatetimeLike | None = None,
        geometry: GeometryLike | None = None,
        point: tuple[float, float] | None = None,
        radius: float | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        cloud_cover: tuple[int, int] | None = None,
        instrument: str | None = None,
        product_type: str | None = None,
        orbit_direction: str | None = None,
        resolution: str | None = None,
        sensor_mode: str | None = None,
        status: str | None = None,
        **kwargs: dict[str, Any],
    ) -> None:
        """Search OpenSearch.

        Args:
            collection (str | None, optional): collection name to search. Defaults to None.
            name (str | None, optional): collection name to search. Defaults to None.
            product_id (str | None, optional): product id to search. Defaults to None.
            date (DatetimeLike | None, optional): sensing datetime / range to search. Defaults to None.
            publication_date (DatetimeLike | None, optional): publication date / range to search. Defaults to None.
            geometry (GeometryLike | None, optional): area to search. Defaults to None.
            point (tuple[float, float] | None, optional): point to search. Defaults to None.
            radius (float | None, optional): radius to buffer point by. Defaults to None.
            bbox (tuple[float, float, float, float] | None, optional): bbox to search. Defaults to None.
            cloud_cover (tuple[int, int] | None, optional): cloud cover range to filter on. Defaults to None.
            instrument (str | None, optional): instrument to filter on. Defaults to None.
            product_type (str | None, optional): product type to filter on. Defaults to None.
            orbit_direction (str | None, optional): orbit direction to filter on. Defaults to None.
            resolution (str | None, optional): resolution to filter on. Defaults to None.
            sensor_mode (str | None, optional): sensor mode to filter on. Defaults to None.
            status (str | None, optional): status to filter on. Defaults to None.

        Keyword Arguments:
            **kwargs: keyword arguments to additionally format
        """
        self.collection = collection
        self.params = format_params(
            name=name,
            product_id=product_id,
            date=date,
            publication_date=publication_date,
            geometry=geometry,
            point=point,
            radius=radius,
            bbox=bbox,
            cloud_cover=cloud_cover,
            instrument=instrument,
            product_type=product_type,
            orbit_direction=orbit_direction,
            resolution=resolution,
            sensor_mode=sensor_mode,
            status=status,
            **kwargs,
        )

    @staticmethod
    def _get(url: str, params: dict[str, Any]) -> dict[str, Any]:
        try:
            response = requests.get(url, params=params)
            handle_response(response)
            content = dict(response.json())
        except Exception as e:
            raise e
        return content

    @staticmethod
    def get_url_for_collection(collection: str | None) -> str:
        """Get search url formatted for collection.

        Args:
            collection (str): collection name

        Returns:
            str: search url for collection
        """
        if collection:
            return f"{SEARCH_BASE_URL}/search.json"
        return f"{SEARCH_BASE_URL}/collections/{collection}/search.json"

    def get(
        self,
        page_size: int = 1000,
        sort_param: str | None = None,
        sort_order: str | None = None,
        limit: int | None = 1000,
    ) -> Generator[dict[str, Any], None, None]:
        """Get search results.

        Args:
            page_size (int, optional): page size to search with. Defaults to 1000.
            sort_param (str | None, optional): attribute to sort on. Defaults to None.
            sort_order (str | None, optional): order to sort products on. Defaults to None.
            limit (int |None, optional): result limit. Defaults to 1000.

        Yields:
            Generator[dict, None, None]: product results
        """
        params = self.params
        params["maxRecords"] = min(page_size, limit) if limit else page_size
        params["sortParam"] = sort_param
        params["sortOrder"] = sort_order

        url = self.get_url_for_collection(self.collection)

        more_results = True
        count = 0
        next_link = None

        while more_results:
            content = self._get(url, params)
            page_results = content.get("features") or []
            links = content.get("properties", {}).get("links") or []

            if links:
                next_link = next(
                    (link for link in links if link.get("rel") == "next"), None
                )
                if next_link:
                    url = next_link.get("href")
                    params = {}
            if next_link is None:
                more_results = False

            for i in page_results:
                if limit is not None and count < limit:
                    yield i
                    count += 1
                else:
                    more_results = False
                    break

    def hits(self) -> dict[str, Any]:
        """Get product hits.

        Returns:
            dict: product hits
        """
        URL = f"{SEARCH_BASE_URL}/collections/{self.collection}/search.json"
        r = requests.get(URL, params=self.params)
        r.raise_for_status()
        return dict(r.json())
