from opengeh_electrical_heating.domain.calculation import CalculatedNames
from opengeh_electrical_heating.domain.column_names import ColumnNames
from opengeh_electrical_heating.infrastructure.electricity_market.child_metering_points.schema import (
    child_metering_points_v1,
)
from opengeh_electrical_heating.infrastructure.electricity_market.consumption_metering_point_periods.schema import (
    consumption_metering_point_periods_v1,
)
from opengeh_electrical_heating.infrastructure.measurements.calculated_measurements.schema import (
    calculated_measurements_schema,
)
from opengeh_electrical_heating.infrastructure.measurements.measurements_gold.schema import (
    time_series_points_v1,
)

# Imports for all other StructTypes in the infrastructure directory
ALL_STRUCT_TYPES = [
    child_metering_points_v1,
    consumption_metering_point_periods_v1,
    calculated_measurements_schema,
    time_series_points_v1,
]


def test_structfield_names_in_columnnames() -> None:
    """ColumnNames should contain all StructField names from all StructTypes."""

    # Get all attribute names from ColumnNames class
    column_names = [
        attr for attr in dir(ColumnNames) if not callable(getattr(ColumnNames, attr)) and not attr.startswith("__")
    ]

    # Check StructField names in all StructTypes
    for struct_type in ALL_STRUCT_TYPES:
        for field in struct_type.fields:
            assert field.name in column_names, f"StructField name '{field.name}' not found in ColumnNames"


def test_columnnames_in_structfields_names() -> None:
    """
    All ColumnNames values should be in StructType field names.
    Otherwise they must be removed, or moved to _CalculatedNames.
    """

    # Get all attribute names from ColumnNames class
    column_names = [
        attr for attr in dir(ColumnNames) if not callable(getattr(ColumnNames, attr)) and not attr.startswith("__")
    ]

    # Collect all field names from all StructTypes
    struct_field_names = set()
    for struct_type in ALL_STRUCT_TYPES:
        for field in struct_type.fields:
            struct_field_names.add(field.name)

    # Check that all ColumnNames values are in struct field names
    for name in column_names:
        assert name in struct_field_names, f"ColumnNames value '{name}' not found in any StructType fields"


def test_no_overlap_between_columnnames_and_calculatednames() -> None:
    """Column names and calculated names should not overlap."""

    # Get all attribute names from ColumnNames and _CalculatedNames classes
    column_names = [
        attr for attr in dir(ColumnNames) if not callable(getattr(ColumnNames, attr)) and not attr.startswith("__")
    ]
    calculated_names = [
        attr
        for attr in dir(CalculatedNames)
        if not callable(getattr(CalculatedNames, attr)) and not attr.startswith("__")
    ]

    # Check for overlap
    overlap = set(column_names) & set(calculated_names)
    assert not overlap, f"Overlap found between ColumnNames and _CalculatedNames: {overlap}"


def test_columnnames_attributes_are_sorted() -> None:
    """ColumnNames attributes should be sorted alphabetically."""

    # Get all attribute names from ColumnNames class
    column_names = [
        attr for attr in dir(ColumnNames) if not callable(getattr(ColumnNames, attr)) and not attr.startswith("__")
    ]

    # Check if the column names are sorted
    assert column_names == sorted(column_names), "ColumnNames attributes are not sorted alphabetically"


def test_calculatednames_attributes_are_sorted() -> None:
    """_CalculatedNames attributes should be sorted alphabetically."""

    # Get all attribute names from _CalculatedNames class
    calculated_names = [
        attr
        for attr in dir(CalculatedNames)
        if not callable(getattr(CalculatedNames, attr)) and not attr.startswith("__")
    ]

    # Check if the calculated names are sorted
    assert calculated_names == sorted(calculated_names), "_CalculatedNames attributes are not sorted alphabetically"
