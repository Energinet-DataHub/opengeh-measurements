import pytest

from geh_calculated_measurements.common.domain import ContractColumnNames, calculated_measurements_schema
from geh_calculated_measurements.electrical_heating.domain import (
    EphemiralColumnNames,
    child_metering_points_v1,
    consumption_metering_point_periods_v1,
    time_series_points_v1,
)

# Imports for all other StructTypes in the infrastructure directory
ALL_CONTRACT_STRUCT_TYPES = [
    child_metering_points_v1,
    consumption_metering_point_periods_v1,
    calculated_measurements_schema,
    time_series_points_v1,
]


def test_structfield_names_in_contractcolumnnames() -> None:
    """ContractColumnNames should contain all StructField names from all StructTypes."""

    # Get all attribute names from ColumnNames class
    column_names = [
        attr
        for attr in dir(ContractColumnNames)
        if not callable(getattr(ContractColumnNames, attr)) and not attr.startswith("__")
    ]

    # Check StructField names in all StructTypes
    for struct_type in ALL_CONTRACT_STRUCT_TYPES:
        for field in struct_type.fields:
            assert field.name in column_names, f"StructField name '{field.name}' not found in ContractColumnNames"


@pytest.mark.skip(reason="Skipping until we have a strategy for testing this across calculation types")
def test_columnnames_in_structfields_names() -> None:
    """
    All ColumnNames values should be in StructType field names.
    Otherwise they must be removed, or moved to EphemiralColumnNames.
    """

    # Get all attribute names from ColumnNames class
    column_names = [
        attr
        for attr in dir(ContractColumnNames)
        if not callable(getattr(ContractColumnNames, attr)) and not attr.startswith("__")
    ]

    # Collect all field names from all StructTypes
    struct_field_names = set()
    for struct_type in ALL_CONTRACT_STRUCT_TYPES:
        for field in struct_type.fields:
            struct_field_names.add(field.name)

    # Check that all ColumnNames values are in struct field names
    for name in column_names:
        assert name in struct_field_names, f"EphemiralColumnNames value '{name}' not found in any StructType fields"


def test_no_overlap_between_columnnames_and_ephemiralcolumnnames() -> None:
    """Column names and ephemiral names should not overlap."""

    # Get all attribute names from ContractColumnNames and EphemiralColumnNames classes
    column_names = [
        attr
        for attr in dir(ContractColumnNames)
        if not callable(getattr(ContractColumnNames, attr)) and not attr.startswith("__")
    ]
    calculated_names = [
        attr
        for attr in dir(EphemiralColumnNames)
        if not callable(getattr(EphemiralColumnNames, attr)) and not attr.startswith("__")
    ]

    # Check for overlap
    overlap = set(column_names) & set(calculated_names)
    assert not overlap, f"Overlap found between ColumnNames and EphemiralColumnNames: {overlap}"


def test_ephemiralcolumnnames_attributes_are_sorted() -> None:
    """EphemiralColumnNames attributes should be sorted alphabetically."""

    # Get all attribute names from EphemiralColumnNames class
    ephemiral_column_names = [
        attr
        for attr in dir(EphemiralColumnNames)
        if not callable(getattr(EphemiralColumnNames, attr)) and not attr.startswith("__")
    ]

    # Check if the calculated names are sorted
    assert ephemiral_column_names == sorted(ephemiral_column_names), (
        "EphemiralColumnNames attributes are not sorted alphabetically"
    )
