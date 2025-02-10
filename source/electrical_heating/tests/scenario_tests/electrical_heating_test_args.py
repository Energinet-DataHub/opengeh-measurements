from pydantic_settings import SettingsConfigDict

from opengeh_electrical_heating.domain import ElectricalHeatingArgs


class ElectricalHeatingTestArgs(ElectricalHeatingArgs):
    """Args for testing the electrical heating job. Sets the model_config to point to specifi .env file pro"""

    pass
