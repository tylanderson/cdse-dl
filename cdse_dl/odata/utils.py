import requests
from requests.exceptions import HTTPError, JSONDecodeError


class CopernicusODataError(Exception):  # noqa: D101
    ...


def handle_response(response: requests.Response) -> None:
    """Check response for errors.

    Args:
        response (requests.Response): response

    Raises:
        Exception: Invalid Request
    """
    try:
        response.raise_for_status()
    except HTTPError as e:
        try:
            detail = response.json()["detail"]
        except (JSONDecodeError, KeyError):
            detail = response.text
        raise CopernicusODataError(f"Request Failed: {detail}") from e
