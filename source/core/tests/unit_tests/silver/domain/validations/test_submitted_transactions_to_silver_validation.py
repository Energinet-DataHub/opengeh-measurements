import core.silver.domain.validations.enum_validations as enum_validations
import core.silver.domain.validations.submitted_transactions_to_silver_validation as submitted_transactions_to_silver_validation


def test__all_validations_list_contains_all_validations() -> None:
    # Arrange
    expected = [
        enum_validations.validate_orchestration_type_enum,
        enum_validations.validate_quality_enum,
        enum_validations.validate_metering_point_type_enum,
        enum_validations.validate_unit_enum,
        enum_validations.validate_resolution_enum,
    ]

    # Act
    actual = submitted_transactions_to_silver_validation.all_validations_list()

    # Assert
    assert actual == expected
