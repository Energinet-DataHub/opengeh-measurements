from tests import TESTS_ROOT


def get_cenc_test_files_folder_path() -> str:
    return (TESTS_ROOT / "net_consumption_group_6" / "job_tests" / "test_files" / "cenc").as_posix()
