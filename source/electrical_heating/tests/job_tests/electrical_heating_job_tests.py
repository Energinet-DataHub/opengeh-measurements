from unittest.mock import patch

import electrical_heating.application.execute_with_deps as execute_with_deps

DEFAULT_ORCHESTRATION_INSTANCE_ID = "12345678-9fc8-409a-a169-fbd49479d711"


def test_execute_with_deps():
    # Arrange
    sys_argv = ["dummy_script_name", "orchestration_instance_id", DEFAULT_ORCHESTRATION_INSTANCE_ID]

    # Act
    with patch("sys.argv", sys_argv):
        execute_with_deps.execute_with_deps()

    # Assert
