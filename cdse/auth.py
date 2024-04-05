import logging
import threading
import time
from typing import Any, Dict
from urllib.parse import urlparse

import requests
import requests.auth

logger = logging.getLogger(__name__)

AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

AUTH_DOMAINS = [
    "catalogue.dataspace.copernicus.eu",
    "download.dataspace.copernicus.eu",
    "zipper.dataspace.copernicus.eu",
]


def check_response(response: requests.Response):
    """Check token response.

    Args:
        response (requests.Response): response

    Raises:
        Exception: token response error
    """
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        error_info = response.json()
        raise Exception(
            f"Unable to get token. Error: {error_info['error']}. Detail: {error_info['error_description']}"
        ) from e


def response_to_token_info(response: requests.Response) -> Dict:
    """Get token info from response.

    Adds `acquired_time` to token info

    Args:
        response (requests.Response): _description_

    Returns:
        Dict: token info
    """
    token_info = response.json()
    token_info["acquired_time"] = time.time()
    return token_info


def get_token_info(username: str, password: str) -> Dict:
    """Get token info from username password auth.

    Args:
        username (str): username
        password (str): password

    Returns:
        Dict: token info
    """
    logger.debug("getting token")
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    response = requests.post(AUTH_URL, data=data)
    check_response(response)

    return response_to_token_info(response)


def refresh_token_info(refresh_token: str) -> Dict:
    """Refresh token info using refresh token.

    Args:
        refresh_token (str): refresh token

    Returns:
        Dict: fresh token info
    """
    logger.debug("refreshing token")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": "cdse-public",
    }

    response = requests.post(AUTH_URL, headers=headers, data=data)
    check_response(response)
    return response_to_token_info(response)


def refresh_token_info_or_reauth(
    refresh_token: str, username: str, password: str
) -> Dict:
    """Refresh token info using refresh token, and if token is invalid or expired, use username password to re-auth.

    Args:
        refresh_token (str): refresh token
        username (str): username
        password (str): password

    Returns:
        Dict: fresh token info
    """
    try:
        new_token_info = refresh_token_info(refresh_token)
    except Exception:
        new_token_info = get_token_info(username, password)  # *self.username_password)
    return new_token_info


def is_token_expired(acquired_time: float, expires_time: float, buffer: float = 60):
    """Check if token is expired based on acquired time, and expires_time.

    Has a configurable buffer to ensure invalid tokens are not used is delayed

    Args:
        acquired_time (float): acquired time, in seconds since the Epoch.
        expires_time (float): expires time, in seconds since token is issued.
        buffer (float, optional): buffer time, in seconds. Defaults to 60.

    Returns:
        _type_: _description_
    """
    current_time = time.time()
    return (current_time - acquired_time + buffer) >= expires_time


class BearerAuth(requests.auth.AuthBase):
    """Bearer token auth."""

    def __init__(self, token: str):
        """Auth for bearer token."""
        self.token = token

    def __call__(self, r):
        """Adds auth header."""
        r.headers["authorization"] = f"Bearer {self.token}"
        return r


class CDSEAuthSession(requests.Session):
    """authorized cdse session."""

    def __init__(self, username, password, *args, **kwargs):
        """Create an authorzied session to cdse."""
        super().__init__(*args, **kwargs)
        self.username = username
        self.password = password
        self.token_lock = threading.Lock()
        self.token_info = get_token_info(
            self.username, self.password
        )  # Get initial token information using username and password
        self.auth = self._create_auth()

    def _create_auth(self) -> BearerAuth:
        """Create a new TokenAuth instance with the current access token."""
        return BearerAuth(self.token_info["access_token"])

    def refresh_token(self):
        """Refresh the access token using the refresh token."""
        with self.token_lock:
            # )
            new_token_info = refresh_token_info_or_reauth(
                refresh_token=self.token_info["refresh_token"],
                username=self.username,
                password=self.password,
            )
            self.token_info.update(new_token_info)
            self.auth = self._create_auth()

    def is_token_expired(self):
        """Check if the current access token has expired."""
        return is_token_expired(
            self.token_info["acquired_time"], self.token_info["expires_in"]
        )

    def rebuild_auth(self, prepared_request: Any, response: Any):
        """Keep headers upon redirect as long as we are on any of AUTH_DOMAINS."""
        headers = prepared_request.headers
        url = prepared_request.url

        if "Authorization" in headers:
            original_parsed = urlparse(response.request.url)
            redirect_parsed = urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and (
                redirect_parsed.hostname not in AUTH_DOMAINS
                or original_parsed.hostname not in AUTH_DOMAINS
            ):
                logger.debug(
                    f"Deleting Auth Headers: {original_parsed.hostname} -> {redirect_parsed.hostname}"
                )
                del headers["Authorization"]

    def request(self, *args, **kwargs):
        """Auto-refreshing authenticated request."""
        if self.is_token_expired():
            logger.debug("token is expired")
            self.refresh_token()

        response = super().request(*args, **kwargs)

        if response.status_code == 401:  # Unauthorized, try re-authenticating directly
            logger.debug("401 status, re-trying auth")
            self.refresh_token()
            response = super().request(*args, **kwargs)
        return response
