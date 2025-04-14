from geh_calculated_measurements.common.domain import ContractColumnNames


def test_contractcolumnnames_attributes_are_sorted() -> None:
    """ContractColumnNames attributes should be sorted alphabetically."""

    # Get all attribute names from ColumnNames class
    column_names = [
        attr
        for attr in dir(ContractColumnNames)
        if not callable(getattr(ContractColumnNames, attr)) and not attr.startswith("__")
    ]

    # Check if the column names are sorted
    assert column_names == sorted(column_names), "test_contract_column_names attributes are not sorted alphabetically"
