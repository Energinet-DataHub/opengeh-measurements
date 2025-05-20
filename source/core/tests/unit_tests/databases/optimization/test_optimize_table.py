from pytest_mock import MockFixture

import core.databases.optimization.optimize_table as sut
import core.databases.spark_session as spark_session


def test__optimize_table__executes_optimize_on_table(mocker: MockFixture) -> None:
    # Arrange
    database = "test_db"
    table = "test_table"
    mocked_spark = mocker.Mock()
    mocker.patch(f"{spark_session.__name__}.{spark_session.initialize_spark.__name__}", return_value=mocked_spark)

    # Act
    sut.optimize_table(database, table)

    # Assert
    mocked_spark.sql.assert_called_once_with(f"OPTIMIZE {database}.{table}")
