from enum import Enum


class Quality(Enum):
    Q_UNSPECIFIED = "Q_UNSPECIFIED"
    Q_MISSING = "Q_MISSING"
    Q_ESTIMATED = "Q_ESTIMATED"
    Q_MEASURED = "Q_MEASURED"
    Q_CALCULATED = "Q_CALCULATED"
