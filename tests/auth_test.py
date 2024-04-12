from unittest.mock import patch

import pytest

from cdse_dl.auth import APIAuthException, get_token_info, is_token_expired


def test_get_token_info(requests_mock):
    requests_mock.post(
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        json={
            "error": "invalid_grant",
            "error_description": "Invalid user credentials",
        },
        status_code=401,
    )

    with pytest.raises(APIAuthException) as e:
        get_token_info("username", "password")
    assert (
        str(e.value)
        == "Unable to get token. Error: invalid_grant. Detail: Invalid user credentials"
    )

    requests_mock.post(
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        json={"error": "unknown_error"},
        status_code=500,
    )
    with pytest.raises(APIAuthException) as e:
        get_token_info("username", "password")
    assert str(e.value) == "Unable to get token. Error: unknown_error. Detail: None"

    mock_time_stamp = 1609459200
    with patch("time.time", return_value=mock_time_stamp):
        requests_mock.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            json={
                "access_token": "token",
                "expires_in": 600,
                "refresh_expires_in": 3600,
                "refresh_token": "refresh_token",
                "token_type": "Bearer",
                "not-before-policy": 0,
                "session_state": "state",
                "scope": "AUDIENCE_PUBLIC openid email profile ondemand_processing user-context",
            },
        )
        info = get_token_info("username", "password")
        assert info["acquired_time"] == mock_time_stamp


def test_is_token_expired():
    mock_time_stamp = 1000

    with patch("time.time", return_value=mock_time_stamp):
        # token is expired already
        is_expired = is_token_expired(100, 100, 10)
        assert is_expired is True
        # token is still valid past buffer
        is_expired = is_token_expired(999, 100, 10)
        assert is_expired is False
        # token is still valid, but past buffer time
        is_expired = is_token_expired(999, 10, 10)
        assert is_expired is True
