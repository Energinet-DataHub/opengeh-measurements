from tests import TESTS_ROOT


def get_test_files_folder_path() -> str:
    return (TESTS_ROOT / "net_consumption_group_6" / "job_tests" / "test_files").as_posix()
