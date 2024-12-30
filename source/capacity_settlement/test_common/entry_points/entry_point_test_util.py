import importlib.metadata
from importlib.metadata import PackageNotFoundError
from typing import Any


def assert_entry_point_exists(entry_point_name: str, module: Any) -> None:
    """
    Asserts that a given entry point exists and its corresponding function
    is present in the specified module.

    Args:
        entry_point_name (str): The name of the entry point to check.
        module (Any): The module object to verify the function's existence.

    Raises:
        AssertionError: If the entry point or function does not exist.
    """
    try:

        # Retrieve the console script entry point group
        entry_point_group = importlib.metadata.entry_points(group="console_scripts")
        entry_point = next(
            (ep for ep in entry_point_group if ep.name == entry_point_name), None
        )

        # Check if the entry point exists
        if not entry_point:
            raise AssertionError(f"The entry point '{entry_point_name}' was not found.")

        # Extract module and function names
        module_name, function_name = entry_point.value.split(":")

        # Validate the function existence in the provided module
        if not hasattr(
            module,
            function_name,
        ):
            raise AssertionError(
                f"The function '{function_name}' does not exist in the provided module."
            )

        # Import the module to ensure it is present
        importlib.import_module(module_name)
    except PackageNotFoundError:
        raise AssertionError(f"The entry point group 'console_scripts' was not found.")
