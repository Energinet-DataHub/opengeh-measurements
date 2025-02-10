class DatabaseNames:
    # Should probably be an environment variable because it is created in Terraform.
    bronze_database = "measurements_bronze"
    silver_migrations_database = "migrations_silver"
    measurements_internal_database = "measurements_internal"
