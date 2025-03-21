from pathlib import Path

from geh_common.testing.covernator import create_all_cases_file, create_result_and_all_scenario_files

output_folder = Path("/workspace/source/geh_calculated_measurements/src/covernator/output_files")
create_all_cases_file(
    output_folder,
    Path(
        "/workspace/source/geh_calculated_measurements/tests/missing_measurements_log/coverage/all_cases_missing_measurements_log.yml"
    ),
)
create_result_and_all_scenario_files(
    output_folder,
    Path("/workspace/source/geh_calculated_measurements/tests/missing_measurements_log/scenario_tests"),
)
