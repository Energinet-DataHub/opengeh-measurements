# Lifted from opengeh-migrations.
from enum import Enum


class Dh2QualityEnum(Enum):
    measured = "E01"  # The old naming for E01 is "read".
    estimated = "56"
    calculated = "D01"
    missing = "QM"
    revised = "36"
