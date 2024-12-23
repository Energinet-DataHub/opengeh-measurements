import database_migration.migrations as migrations


def test__migrations():
    # Act
    migrations.migrate()

    # Assert
    assert True
