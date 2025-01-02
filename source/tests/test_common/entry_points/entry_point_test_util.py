﻿import importlib.metadata
import pkgutil
from typing import Any


def list_all_modules(package_name: str):
    package = __import__(package_name, fromlist=[""])
    for module_info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        print(module_info.name)


def assert_entry_point_exists(entry_point_name: str, module: Any) -> None:
    try:
        # Arrange
        # filter on module name

        list_all_modules("source")

        entry_point = importlib.metadata.entry_points(
            group="console_scripts", name=entry_point_name
        )

        # Convert to list, remove the first element, and convert back to tuple
        entry_points_list = list(entry_point)
        if entry_points_list:
            entry_points_list.pop(0)
        entry_point = tuple(entry_points_list)

        print(module.__name__)
        print(entry_points_list)

        # Check if the entry point exists
        if not entry_point:
            assert False, f"The {entry_point_name} entry point was not found."

        # Check if the module exists
        module_name = entry_point[0].module
        function_name = entry_point[0].value.split(":")[1]

        print(module_name)
        print(function_name)

        if not hasattr(
            module,
            function_name,
        ):
            assert (
                False
            ), f"The entry point module function {function_name} does not exist in the entry points file."

        importlib.import_module(module_name)
    except importlib.metadata.PackageNotFoundError:
        assert False, f"The {entry_point_name} entry point was not found."