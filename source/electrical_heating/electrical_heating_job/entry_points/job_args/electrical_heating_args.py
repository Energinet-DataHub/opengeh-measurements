from dataclasses import dataclass


@dataclass
class ElectricalHeatingArgs:
    """
    Args for the electrical heating job.
    """

    electrical_heating_id: str
