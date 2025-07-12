"""odata types."""

from enum import StrEnum
from typing import Any, Literal, NotRequired, TypedDict


class SubscriptionType(StrEnum):  # noqa: D101
    PULL = "pull"
    PUSH = "push"


class Attribute(TypedDict):  # noqa: D101
    Name: str
    ValueType: Literal["String", "Integer", "Double", "DateTimeOffset", "Boolean"]


CollectionAttributes = dict[str, list[Attribute]]

SubscriptionEventType = Literal["created", "modified", "created, modified", "deleted"]
SubscriptionStatus = Literal["running", "paused", "canceled"]


class SubscriptionInfo(TypedDict, total=False):  # noqa: D101
    Id: str
    FilterParam: str
    StageOrder: bool
    Priority: int
    Status: SubscriptionStatus
    LastNotificationDate: NotRequired[str]  # ISO 8601 date string
    SubscriptionEvent: list[SubscriptionEventType]
    SubmissionDate: str  # ISO 8601 date string
    NotificationEndpoint: NotRequired[str]


class SubscriptionEntity(TypedDict, total=False):  # noqa: D101
    SubscriptionEvent: SubscriptionEventType
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
