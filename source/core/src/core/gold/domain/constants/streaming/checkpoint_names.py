from enum import Enum


class CheckpointNames(Enum):
    SILVER_TO_GOLD = "measurements"
    CALCULATED_TO_GOLD = "measurements_calculated"
    MIGRATIONS_TO_GOLD = "measurements_migrations"
