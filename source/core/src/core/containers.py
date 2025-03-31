from dependency_injector import containers, providers

from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.silver.infrastructure.repositories.silver_measurements_repository import (
    SilverMeasurementsRepository,
)


class Container(containers.DeclarativeContainer):
    # Repositories
    gold_measurements_repository = providers.Singleton(GoldMeasurementsRepository)
    silver_measurements_repository = providers.Singleton(SilverMeasurementsRepository)
