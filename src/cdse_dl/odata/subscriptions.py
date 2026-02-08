"""CDSE subscriptions management tooling."""

from typing import Any

from cdse_dl.auth import CDSEAuthSession, Credentials
from cdse_dl.odata.constants import ODATA_BASE_URL
from cdse_dl.odata.filter import Filter
from cdse_dl.odata.types import (
    AckInfo,
    SubscriptionEntity,
    SubscriptionEventType,
    SubscriptionInfo,
    SubscriptionStatus,
    SubscriptionType,
)
from cdse_dl.odata.utils import handle_response

SUBSCRIPTIONS_URL = ODATA_BASE_URL + "/Subscriptions"


def _build_subscription_body(
    subscription_type: SubscriptionType | None = None,
    *,
    filter: Filter | None = None,
    status: SubscriptionStatus | None = None,
    event_types: list[SubscriptionEventType] | None = None,
    notification_endpoint: str | None = None,
    endpoint_username: str | None = None,
    endpoint_password: str | None = None,
) -> dict[str, Any]:
    """Build subscription body for creation or modification.

    Args:
        subscription_type (SubscriptionType | None, optional): Subscription type to create (push or pull). Required for creation, not used for modification. Defaults to None.
        filter (Filter | None, optional): OData Filter used to filter products that go to the subscription. Defaults to None.
        status (SubscriptionStatus | None, optional): status of subscription. Defaults to None.
        event_types (list[SubscriptionEventType] | None, optional): event types to subscribe to, defaults to "created". Defaults to None.
        notification_endpoint (str | None, optional): notification endpoint for push subscriptions. Defaults to None.
        endpoint_username (str | None, optional): notification endpoint username for push subscriptions. Defaults to None.
        endpoint_password (str | None, optional): notification endpoint password for push subscriptions. Defaults to None.


    Returns:
        dict[str, Any]: subscription body
    """
    body: dict[str, Any] = {}

    if subscription_type:
        body["SubscriptionType"] = subscription_type
    if event_types:
        body["SubscriptionEvent"] = event_types
    if filter:
        body["FilterParam"] = filter.filter_string
    if status:
        body["Status"] = status
    if notification_endpoint:
        body["NotificationEndpoint"] = notification_endpoint
    if endpoint_username:
        body["NotificationEpUsername"] = endpoint_username
    if endpoint_password:
        body["NotificationEpPassword"] = endpoint_password

    return body


class SubscriptionClient:
    """Client for managing CDSE subscriptions."""

    def __init__(self, credentials: Credentials | None = None) -> None:
        """Client for managing CDSE subscriptions.

        Args:
            credentials (Credentials | None, optional): CDSE credentials. Defaults to None.
        """
        self.session = CDSEAuthSession(credentials)

    def create_subscription(
        self,
        subscription_type: SubscriptionType,
        filter: Filter | None = None,
        event_types: list[SubscriptionEventType] | None = None,
        notification_endpoint: str | None = None,
        endpoint_username: str | None = None,
        endpoint_password: str | None = None,
    ) -> SubscriptionInfo:
        """Create subscription.

        Args:
            subscription_type (SubscriptionType): Subscription type to create (push or pull)
            filter (Filter | None, optional): OData Filter used to filter products that go to the subscription. Defaults to None.
            event_types (list[SubscriptionEventType] | None, optional): event types to subscribe to, defaults to "created". Defaults to None.
            notification_endpoint (str | None, optional): notification endpoint for push subscriptions. Defaults to None.
            endpoint_username (str | None, optional): notification endpoint username for push subscriptions. Defaults to None.
            endpoint_password (str | None, optional): notification endpoint password for push subscriptions. Defaults to None.

        Returns:
            SubscriptionInfo: created subscription info
        """
        body = _build_subscription_body(
            filter=filter,
            subscription_type=subscription_type,
            event_types=event_types,
            notification_endpoint=notification_endpoint,
            endpoint_username=endpoint_username,
            endpoint_password=endpoint_password,
        )

        r = self.session.post(SUBSCRIPTIONS_URL, json=body)
        handle_response(r)
        info: SubscriptionInfo = r.json()
        return info

    def delete_subscription(self, subscription_id: str) -> None:
        """Delete subscription.

        Args:
            subscription_id (str): subscription id to delete
        """
        r = self.session.delete(f"{SUBSCRIPTIONS_URL}({subscription_id})")
        handle_response(r)

    def ack_subscription(self, subscription_id: str, ack_token: str) -> AckInfo:
        """Acknowledge subscription result, removing it from the subscription.

        By acknowledging a result below no at the top of the subscription, all results above will also be acknowledged.

        Args:
            subscription_id (str): subscription id
            ack_token (str): result ack token

        Returns:
            AckInfo: subscription info
        """
        r = self.session.post(
            f"{SUBSCRIPTIONS_URL}({subscription_id})/Ack?$ackid={ack_token}"
        )
        handle_response(r)
        info: AckInfo = r.json()
        return info

    def read_subscription(
        self, subscription_id: str, limit: int = 1
    ) -> list[SubscriptionEntity]:
        """Read subscription.

        Args:
            subscription_id (str): subscription id to read
            limit (int, optional): result limit, max 20. Defaults to 1.

        Returns:
            list[SubscriptionEntity]: subscription results
        """
        r = self.session.get(
            f"{SUBSCRIPTIONS_URL}({subscription_id})/Read?$top={limit}"
        )
        handle_response(r)
        entities: list[SubscriptionEntity] = r.json()
        return entities

    def update_subscription(
        self,
        subscription_id: str,
        status: SubscriptionStatus | None = None,
        notification_endpoint: str | None = None,
        endpoint_username: str | None = None,
        endpoint_password: str | None = None,
    ) -> SubscriptionInfo:
        """Update subscription.

        Args:
            subscription_id (str): subscription id
            status (SubscriptionStatus | None, optional): status of subscription. Defaults to None.
            notification_endpoint (str | None, optional): notification endpoint for push subscriptions. Defaults to None.
            endpoint_username (str | None, optional): notification endpoint username for push subscriptions. Defaults to None.
            endpoint_password (str | None, optional): notification endpoint password for push subscriptions. Defaults to None.

        Returns:
            SubscriptionInfo: updated subscription info
        """
        body = _build_subscription_body(
            status=status,
            notification_endpoint=notification_endpoint,
            endpoint_username=endpoint_username,
            endpoint_password=endpoint_password,
        )

        r = self.session.patch(f"{SUBSCRIPTIONS_URL}({subscription_id})", json=body)
        handle_response(r)
        info: SubscriptionInfo = r.json()
        return info

    def subscription_info(self, subscription_id: str) -> SubscriptionInfo:
        """Get subscription info.

        Args:
            subscription_id (str): subscription id

        Returns:
            SubscriptionInfo: subscription info
        """
        r = self.session.get(f"{SUBSCRIPTIONS_URL}({subscription_id})")
        handle_response(r)
        info: SubscriptionInfo = r.json()
        return info

    def list_subscriptions(self) -> list[SubscriptionInfo]:
        """List subscriptions.

        Returns:
            list[SubscriptionInfo]: list of subscription infos
        """
        r = self.session.get(f"{SUBSCRIPTIONS_URL}/Info")
        handle_response(r)
        info: list[SubscriptionInfo] = r.json()
        return info
