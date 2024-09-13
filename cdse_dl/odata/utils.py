import requests


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
