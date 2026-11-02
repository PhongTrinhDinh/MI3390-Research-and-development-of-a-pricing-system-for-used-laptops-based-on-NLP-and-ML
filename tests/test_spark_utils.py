import os
import sys
from unittest.mock import MagicMock, patch

from src.utils.spark_utils import setup_spark_env, get_spark_session


def test_setup_spark_env_pyspark_python_and_java_home(monkeypatch):
    """Kiểm tra setup_spark_env tự động gán PYSPARK_PYTHON và chuẩn hóa JAVA_HOME."""
    monkeypatch.setenv("JAVA_HOME", r"C:\Program Files\Java\jdk-17\bin")
    monkeypatch.delenv("PYSPARK_PYTHON", raising=False)
    monkeypatch.delenv("PYSPARK_DRIVER_PYTHON", raising=False)

    setup_spark_env()

    # 1. Trỏ trực tiếp về sys.executable
    assert os.environ["PYSPARK_PYTHON"] == sys.executable
    assert os.environ["PYSPARK_DRIVER_PYTHON"] == sys.executable

    # 2. Xóa bỏ đuôi \bin của JAVA_HOME
    assert os.environ["JAVA_HOME"] == r"C:\Program Files\Java\jdk-17"


def test_setup_spark_env_hadoop_home_detection(monkeypatch):
    """Kiểm tra setup_spark_env tự động nhận diện HADOOP_HOME nếu C:\\hadoop tồn tại."""
    monkeypatch.delenv("HADOOP_HOME", raising=False)

    with patch("os.path.exists", side_effect=lambda path: path == r"C:\hadoop"):
        setup_spark_env()
        assert os.environ["HADOOP_HOME"] == r"C:\hadoop"


def test_get_spark_session_factory():
    """Kiểm tra factory get_spark_session khởi tạo đúng appName và chuẩn nén snappy."""
    with patch("src.utils.spark_utils.SparkSession") as mock_spark_cls:
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_session = MagicMock()
        mock_builder.getOrCreate.return_value = mock_session

        session = get_spark_session("Test_App_Name")

        assert session == mock_session
        mock_builder.appName.assert_called_once_with("Test_App_Name")
        mock_builder.config.assert_called_once_with("spark.sql.parquet.compression.codec", "snappy")
        mock_builder.getOrCreate.assert_called_once()

