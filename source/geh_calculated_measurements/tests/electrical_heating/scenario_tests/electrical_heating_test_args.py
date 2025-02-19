from geh_calculated_measurements.electrical_heating.application import ElectricalHeatingArgs


class ElectricalHeatingTestArgs(ElectricalHeatingArgs):
    """Args for testing the electrical heating job."""

    def __init__(self, env_file_path: str) -> None:
        super().__init__(_env_file=env_file_path)
