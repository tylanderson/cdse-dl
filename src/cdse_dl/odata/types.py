"""odata types."""

from typing import Any, Literal, NotRequired, TypedDict


class Attribute(TypedDict):  # noqa: D101
    Name: str
    ValueType: Literal["String", "Integer", "Double", "DateTimeOffset", "Boolean"]


CollectionAttributes = dict[str, list[Attribute]]

SubscriptionType = Literal["created", "modified", "created, modified", "deleted"]
SubscriptionStatus = Literal["running", "paused", "canceled"]


class SubscriptionInfo(TypedDict, total=False):  # noqa: D101
    Id: str
    FilterParam: str
    StageOrder: bool
    Priority: int
    Status: SubscriptionStatus
    LastNotificationDate: NotRequired[str]  # ISO 8601 date string
    SubscriptionEvent: list[SubscriptionType]
    SubmissionDate: str  # ISO 8601 date string
    NotificationEndpoint: NotRequired[str]


class SubscriptionEntity(TypedDict, total=False):  # noqa: D101
    SubscriptionEvent: SubscriptionType
    ProductId: str
    ProductName: NotRequired[str]  # not on PULL after 3 days
    SubscriptionId: str
    NotificationDate: str  # ISO 8601 date string
    AckId: NotRequired[str]  # not on PULL
    value: NotRequired[dict[str, Any]]


class AckInfo(TypedDict, total=False):  # noqa: D101
    AckMessagesNum: int
    CurrentQueueLength: int
    MaxQueueLength: int
