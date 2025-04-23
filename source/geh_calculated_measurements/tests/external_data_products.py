from dataclasses import dataclass
from typing import Any

from geh_common.data_products.measurements_core.measurements_gold import current_v1 as current


@dataclass
class DataProduct:
    database_name: str
    view_name: str
    schema: Any


class ExternalDataProducts:
    """
    This class contains all external data products used in the project.

    The intention is to have all data product dependencies defined in one place in the test code. This way, it is easier to bump for instance the version of a data product.
    """

    CURRENT_MEASUREMENTS: DataProduct = DataProduct(
        database_name=current.database_name,
        view_name=current.view_name,
        schema=current.schema,
    )

    @staticmethod
    def get_all_database_names() -> list[str]:
        return [
            getattr(getattr(ExternalDataProducts, attr), "database_name")
            for attr in dir(ExternalDataProducts)
            if isinstance(getattr(ExternalDataProducts, attr), DataProduct)
        ]

    @staticmethod
    def get_all_data_products() -> list[DataProduct]:
        return [
            getattr(ExternalDataProducts, attr)
            for attr in dir(ExternalDataProducts)
            if isinstance(getattr(ExternalDataProducts, attr), DataProduct)
        ]
