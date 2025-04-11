import tempfile
from typing import Generator
from unittest.mock import patch

import pytest
from geh_common.testing.spark.spark_test_session import get_spark_test_session
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import core.gold.infrastructure.config.spark as gold_spark
import core.utility.shared_helpers as shared_helpers


@pytest.fixture(scope="session")
def spark(session_mocker: MockerFixture) -> Generator[SparkSession, None, None]:
    extra_packages = [
        "org.apache.spark:spark-protobuf_2.12:3.5.4",
        "org.apache.hadoop:hadoop-azure:3.3.2",
        "org.apache.hadoop:hadoop-common:3.3.2",
        "io.delta:delta-spark_2.12:3.1.0",
        "io.delta:delta-core_2.12:2.3.0",
    ]
    session, _ = get_spark_test_session(extra_packages=extra_packages)
    session_mocker.patch(f"{gold_spark.__name__}.initialize_spark", return_value=session)

    yield session

    session.stop()


@pytest.fixture
def mock_checkpoint_path():
    temp_checkpoint = tempfile.mkdtemp()

    with patch.object(
        shared_helpers, shared_helpers.get_storage_base_path.__name__, return_value=temp_checkpoint
    ) as mock_checkpoint_path:
        yield mock_checkpoint_path
