from pyspark.sql import SparkSession
from dependency_injector import containers, providers
import database_migration


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()
    spark = providers.Singleton(SparkSession.builder.getOrCreate)


def create_and_configure_container() -> None:
    container = Container()

    _configuration(container)

    container.wire(packages=[database_migration])


def _configuration(container: Container) -> None:
    container.config.storage_account.from_env("CATALOG_NAME")
