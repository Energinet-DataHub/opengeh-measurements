from pydantic_settings import SettingsConfigDict

from opengeh_electrical_heating.application.job_args.electrical_heating_args import ElectricalHeatingJobArgs


class ElectricalHeatingTestArgs(ElectricalHeatingJobArgs):
    """Args for testing the electrical heating job."""

    def __init__(self, env_file_path: str) -> None:
        self.__class__.model_config = SettingsConfigDict(env_file=env_file_path)
        super().__init__()
