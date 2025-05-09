import pytest

from geh_calculated_measurements.capacity_settlement import domain as cs
from geh_calculated_measurements.capacity_settlement.application.model import calculations as cs_app
from geh_calculated_measurements.common.application import model as common_app
from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.domain import model as common
from geh_calculated_measurements.electrical_heating import domain as eh
from geh_calculated_measurements.electrical_heating.domain import EphemeralColumnNames
from geh_calculated_measurements.net_consumption_group_6 import domain as ncg6

# Imports for all other StructTypes in the infrastructure directory
ALL_CONTRACT_STRUCT_TYPES = [
    common.CalculatedMeasurementsDaily.schema,
    common.CurrentMeasurements.schema,
    common_app.CalculatedMeasurementsInternal.schema,
    cs.MeteringPointPeriods.schema,
    cs.TenLargestQuantities.schema,
    cs_app.Calculations.schema,
    eh.ChildMeteringPoints.schema,
    eh.ConsumptionMeteringPointPeriods.schema,
    ncg6.Cenc.schema,
    ncg6.ChildMeteringPoints.schema,
    ncg6.ConsumptionMeteringPointPeriods.schema,
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
    Otherwise they must be removed, or moved to EphemeralColumnNames.
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
        assert name in struct_field_names, f"EphemeralColumnNames value '{name}' not found in any StructType fields"


def test_no_overlap_between_columnnames_and_ephemeralcolumnnames() -> None:
    """Column names and ephemeral names should not overlap."""

    # Get all attribute names from ContractColumnNames and EphemeralColumnNames classes
    column_names = [
        attr
        for attr in dir(ContractColumnNames)
        if not callable(getattr(ContractColumnNames, attr)) and not attr.startswith("__")
    ]
    calculated_names = [
        attr
        for attr in dir(EphemeralColumnNames)
        if not callable(getattr(EphemeralColumnNames, attr)) and not attr.startswith("__")
    ]

    # Check for overlap
    overlap = set(column_names) & set(calculated_names)
    assert not overlap, f"Overlap found between ColumnNames and EphemeralColumnNames: {overlap}"


def test_ephemeralcolumnnames_attributes_are_sorted() -> None:
    """EphemeralColumnNames attributes should be sorted alphabetically."""

    # Get all attribute names from EphemeralColumnNames class
    ephemeral_column_names = [
        attr
        for attr in dir(EphemeralColumnNames)
        if not callable(getattr(EphemeralColumnNames, attr)) and not attr.startswith("__")
    ]

    # Check if the calculated names are sorted
    assert ephemeral_column_names == sorted(ephemeral_column_names), (
        "EphemeralColumnNames attributes are not sorted alphabetically"
    )
