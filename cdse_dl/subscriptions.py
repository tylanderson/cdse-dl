"""CDSE subscriptions management tooling."""

from typing import Dict, List, Literal, Optional

from cdse_dl.auth import CDSEAuthSession, Credentials
from cdse_dl.odata.filter import Filter
from cdse_dl.odata.utils import handle_response

SUBSCRIPTIONS_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Subscriptions"


class SubscriptionClient:
    """Client for managing CDSE subscriptions."""

    def __init__(self, credentials: Optional[Credentials] = None) -> None:
        """Client for managing CDSE subscriptions.

        Args:
            credentials (Optional[Credentials], optional): CDSE credentials. Defaults to None.
        """
        self.session = CDSEAuthSession(credentials)

    def create_subscription(
        self,
        filter: Optional[Filter] = None,
        notification_params: Dict = None,
    ) -> Dict:
        """Create subscription.

        Args:
            filter (Optional[Filter], optional): OData Filter used to filter products that go to the subscription. Defaults to None.
            notification_params (Dict, optional): notification params for push subscriptions. Defaults to None.

        Returns:
            Dict: created subscription info
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
        return r.json()

    def delete_subscription(self, subscription_id: str):
        """Delete subscription.

        Args:
            subscription_id (str): subscription id to delete
        """
        r = self.session.delete(f"{SUBSCRIPTIONS_URL}({subscription_id})")
        handle_response(r)

    def ack_subscription(self, subscription_id: str, ack_token: str) -> Dict:
        """Acknowledge subscription result, removing it from the subscription.

        By acknowledging a result below no at the top of the subscription, all results above will also be acknowledged.

        Args:
            subscription_id (str): subscription id
            ack_token (str): result ack token

        Returns:
            Dict: subscription info
        """
        r = self.session.post(
            f"{SUBSCRIPTIONS_URL}({subscription_id})/Ack?$ackid={ack_token}"
        )
        handle_response(r)
        return r.json()

    def read_subscription(self, subscription_id: str, limit: int = 1) -> List[Dict]:
        """Read subscription.

        Args:
            subscription_id (str): subscription id to read
            limit (int, optional): result limit, max 20. Defaults to 1.

        Returns:
            List[Dict]: subscription results
        """
        r = self.session.get(
            f"{SUBSCRIPTIONS_URL}({subscription_id})/Read?$top={limit}"
        )
        handle_response(r)
        return r.json()

    def update_subscription(
        self,
        subscription_id: str,
        status: Optional[Literal["running", "paused", "cancelled"]] = None,
        notification_params: Optional[Dict] = None,
    ) -> Dict:
        """Update subscription.

        Args:
            subscription_id (str): subscription id
            status (Optional[Literal["running", "paused", "cancelled"]], optional): status. Defaults to None.
            notification_params (Optional[Dict], optional): notification endpoint params for push subscriptions. Defaults to None.

        Returns:
            Dict: updated subscription info
        """
        params = {}
        if status:
            params["Status"] = status
        if notification_params:
            params.update(**notification_params)

        r = self.session.patch(f"{SUBSCRIPTIONS_URL}({subscription_id})", json=params)
        handle_response(r)
        return r.json()

    def subscription_info(self, subscription_id: str) -> Dict:
        """Get subscription info.

        Args:
            subscription_id (str): subscription id

        Returns:
            Dict: subscription info
        """
        r = self.session.get(f"{SUBSCRIPTIONS_URL}({subscription_id})")
        handle_response(r)
        return r.json()

    def list_subscriptions(self) -> List[Dict]:
        """List subscriptions.

        Returns:
            List[Dict]: list of subscription infos
        """
        r = self.session.get(f"{SUBSCRIPTIONS_URL}/Info")
        handle_response(r)
        return r.json()
