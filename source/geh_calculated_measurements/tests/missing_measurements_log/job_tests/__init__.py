from tests import TESTS_ROOT


def get_test_files_folder_path() -> str:
    return (TESTS_ROOT / "electrical_heating" / "job_tests" / "test_files").as_posix()
