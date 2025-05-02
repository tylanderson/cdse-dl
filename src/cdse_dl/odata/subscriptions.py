"""CDSE subscriptions management tooling."""

from typing import Any, Literal

from cdse_dl.auth import CDSEAuthSession, Credentials
from cdse_dl.odata.constants import ODATA_BASE_URL
from cdse_dl.odata.filter import Filter
from cdse_dl.odata.types import AckInfo, SubscriptionEntity, SubscriptionInfo
from cdse_dl.odata.utils import handle_response

SUBSCRIPTIONS_URL = ODATA_BASE_URL + "/Subscriptions"


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
        filter: Filter | None = None,
        notification_params: dict[str, Any] | None = None,
    ) -> SubscriptionInfo:
        """Create subscription.

        Args:
            filter (Filter | None, optional): OData Filter used to filter products that go to the subscription. Defaults to None.
            notification_params (dict, optional): notification params for push subscriptions. Defaults to None.

        Returns:
            dict: created subscription info
        """
        params = {
            "StageOrder": True,  # only available option
            "Priority": 1,  # only available option
            "Status": "running",
            "SubscriptionEvent": [
                "created"  # only available subscription event
            ],
        }

        if filter:
            params["FilterParam"] = filter.filter_string
        if notification_params:
            params.update(**notification_params)

        r = self.session.post(SUBSCRIPTIONS_URL, json=params)
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
            dict: subscription info
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
            list[dict]: subscription results
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
        status: Literal["running", "paused", "cancelled"] | None = None,
        notification_params: dict[str, Any] | None = None,
    ) -> SubscriptionInfo:
        """Update subscription.

        Args:
            subscription_id (str): subscription id
            status (Literal["running", "paused", "cancelled"] | None, optional): status. Defaults to None.
            notification_params (dict | None, optional): notification endpoint params for push subscriptions. Defaults to None.

        Returns:
            dict: updated subscription info
        """
        params = {}
        if status:
            params["Status"] = status
        if notification_params:
            params.update(**notification_params)

        r = self.session.patch(f"{SUBSCRIPTIONS_URL}({subscription_id})", json=params)
        handle_response(r)
        info: SubscriptionInfo = r.json()
        return info

    def subscription_info(self, subscription_id: str) -> SubscriptionInfo:
        """Get subscription info.

        Args:
            subscription_id (str): subscription id

        Returns:
            dict: subscription info
        """
        r = self.session.get(f"{SUBSCRIPTIONS_URL}({subscription_id})")
        handle_response(r)
        info: SubscriptionInfo = r.json()
        return info

    def list_subscriptions(self) -> list[SubscriptionInfo]:
        """List subscriptions.

        Returns:
            list[dict]: list of subscription infos
        """
        r = self.session.get(f"{SUBSCRIPTIONS_URL}/Info")
        handle_response(r)
        info: list[SubscriptionInfo] = r.json()
        return info
