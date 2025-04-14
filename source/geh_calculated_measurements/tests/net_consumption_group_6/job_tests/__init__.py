from tests import TESTS_ROOT


def cnc_get_test_files_folder_path() -> str:
    return (TESTS_ROOT / "net_consumption_group_6" / "job_tests" / "test_files" / "cenc").as_posix()


def cnc_get_test_files_folder_path_with_file_name() -> str:
    return (TESTS_ROOT / "net_consumption_group_6" / "job_tests" / "test_files" / "cnc").as_posix()
