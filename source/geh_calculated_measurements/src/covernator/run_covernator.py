from pathlib import Path

from geh_common.testing.covernator import run_covernator

output_folder = Path("/workspace/source/geh_calculated_measurements/src/covernator/output_files")

run_covernator(output_folder, Path("/workspace/source/geh_calculated_measurements/tests"))
