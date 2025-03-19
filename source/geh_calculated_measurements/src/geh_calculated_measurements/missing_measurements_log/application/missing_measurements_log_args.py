import re
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from geh_common.application.settings import ApplicationSettings
from pydantic import Field, field_validator
from pydantic_settings import NoDecode


class MissingMeasurementsLogArgs(ApplicationSettings):
    orchestration_instance_id: UUID = Field(init=False)
    period_start_datetime: datetime = Field(init=False)
    period_end_datetime: datetime = Field(init=False)
    grid_area_codes: Annotated[list[str], NoDecode] | None = Field(init=False, default=None)
    time_zone: str = "Europe/Copenhagen"
    catalog_name: str = Field(init=False)

    @field_validator("grid_area_codes", mode="before")
    @classmethod
    def _convert_grid_area_codes(cls, value: Any) -> list[str] | None:
        if not value:
            return None
        if isinstance(value, list):
            return [str(item) for item in value]
        else:
            return re.findall(r"\d+", value)

    @field_validator("grid_area_codes", mode="after")
    @classmethod
    def validate_grid_area_codes(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        for code in v:
            assert isinstance(code, str), f"Grid area codes must be strings, not {type(code)}"
            if len(code) != 3 or not code.isdigit():
                raise ValueError(
                    f"Unknown grid area code: '{code}'. Grid area codes must consist of 3 digits (000-999)."
                )
        return v
