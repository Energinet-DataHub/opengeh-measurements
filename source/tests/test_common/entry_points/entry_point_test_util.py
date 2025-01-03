import importlib.metadata
from typing import Any


def assert_entry_point_exists(entry_point_name: str, module: Any) -> None:
    try:
        entry_points = importlib.metadata.entry_points(
            group="console_scripts", name=entry_point_name
        )

        # Check if the entry point exists
        if not entry_points:
            raise AssertionError(
                f"The {entry_point_name} entry point was not found in the module {module.__name__}."
            )

        # Check if the module exists
        module_name = entry_points[entry_point_name].module
        function_name = entry_points[entry_point_name].value.split(":")[1]

        if not hasattr(
            module,
            function_name,
        ):
            raise AssertionError(
                f"The entry point module function {function_name} does not exist in the entry points file."
            )

        importlib.import_module(module_name)
    except importlib.metadata.PackageNotFoundError:
        raise AssertionError(f"The {entry_point_name} entry point was not found.")
