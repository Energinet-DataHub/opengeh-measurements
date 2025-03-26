from enum import Enum


class CombinedStatusEnum(Enum):
    VALID = "Valid"
    DELETED_AND_CANCELED = "Deleted and Cancelled"
    DELETED = "Deleted"
    CANCELLED = "Cancelled"
