def get_substitutions(catalog_name: str, is_testing: bool) -> dict[str, str]:
    """Get the list of substitutions for the migration scripts.
    This is the authoritative list of substitutions that can be used in all migration scripts.
    """
    return {
        "{CATALOG_NAME}": catalog_name,
        # Flags
        "{DATABRICKS-ONLY}": (
            "--" if is_testing else ""
        ),  # Comment out script lines when running in test as it is not using Databricks
        "{TEST-ONLY}": (
            "--" if not is_testing else ""
        ),  # Comment out script lines when running in test as it is not using Databricks
    }
