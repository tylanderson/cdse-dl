from typing import Any, Dict, Optional, Tuple

import requests

from cdse_dl.types import DatetimeLike, GeometryLike
from cdse_dl.utils import parse_datetime_to_components

SEARCH_BASE_URL = "https://catalogue.dataspace.copernicus.eu/resto/api"


def snake_to_camel(snake_str):
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
        print(response.json())
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
    name: Optional[str] = None,
    product_id: Optional[str] = None,
    date: Optional[DatetimeLike] = None,
    publication_date: Optional[DatetimeLike] = None,
    geometry: Optional[GeometryLike] = None,
    point: Optional[Tuple[float, float]] = None,
    radius: Optional[float] = None,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    cloud_cover: Optional[Tuple[int, int]] = None,
    instrument: Optional[str] = None,
    product_type: Optional[str] = None,
    orbit_direction: Optional[str] = None,
    resolution: Optional[str] = None,
    sensor_mode: Optional[str] = None,
    status: Optional[str] = None,
    **kwargs,
):
    params: Dict[str, Any] = {}

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
            params[snake_to_camel(k)] = v

    return params


class ProductSearch:
    def __init__(
        self,
        collection: Optional[str] = None,
        name: Optional[str] = None,
        product_id: Optional[str] = None,
        date: Optional[DatetimeLike] = None,
        publication_date: Optional[DatetimeLike] = None,
        geometry: Optional[GeometryLike] = None,
        point: Optional[Tuple[float, float]] = None,
        radius: Optional[float] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        cloud_cover: Optional[Tuple[int, int]] = None,
        instrument: Optional[str] = None,
        product_type: Optional[str] = None,
        orbit_direction: Optional[str] = None,
        resolution: Optional[str] = None,
        sensor_mode: Optional[str] = None,
        status: Optional[str] = None,
        **kwargs,
    ):
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
    def _get(url, params):
        try:
            response = requests.get(url, params=params)
            handle_response(response)
            content = response.json()
            print(content)
        except Exception as e:
            raise e
        return content

    @staticmethod
    def get_url_for_collection(collection):
        return f"{SEARCH_BASE_URL}/collections/{collection}/search.json"

    def get(
        self,
        page_size: int = 1000,
        sort_param: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = 1000,
    ):
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

    def hits(self):
        URL = f"{SEARCH_BASE_URL}/collections/{self.collection}/search.json"
        r = requests.get(URL, params=self.params)
        r.raise_for_status()
        return r.json()
