from unittest.mock import MagicMock, patch
import pytest

from src.data_pipeline.bronze_layer import (
    create_spark_session,
    ingest_bronze_layer,
    _sanitize_documents,
)


@pytest.fixture
def mock_env(monkeypatch):
    """Fixture thiết lập các biến môi trường phục vụ kiểm thử."""
    env_vars = {
        "MONGO_URI": "mongodb://localhost:27017",
        "MONGO_DB": "chotot_db",
        "MONGO_COLL_RAW": "raw_laptops",
        "DATALAKE_BRONZE_DIR": "/datalake/bronze/laptops",
    }
    for k, v in env_vars.items():
        monkeypatch.setenv(k, v)
    return env_vars


@pytest.fixture
def mock_spark_pipeline():
    """Fixture tạo cây mock hoàn chỉnh cho các đối tượng của PySpark trong pipeline Bronze."""
    mock_spark = MagicMock(name="SparkSession")
    mock_df_raw = MagicMock(name="DataFrameRaw")
    mock_df_transformed = MagicMock(name="DataFrameBronze")
    mock_writer = MagicMock(name="DataFrameWriter")

    # spark.createDataFrame chain
    mock_spark.createDataFrame.return_value = mock_df_raw

    # df_raw count & withColumn chain
    mock_df_raw.count.return_value = 150
    mock_df_raw.withColumn.return_value = mock_df_transformed
    mock_df_transformed.count.return_value = 150
    mock_df_transformed.withColumn.return_value = mock_df_transformed

    # df_bronze.write chain
    mock_df_transformed.write.format.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.partitionBy.return_value = mock_writer

    return {
        "spark": mock_spark,
        "df_raw": mock_df_raw,
        "df_transformed": mock_df_transformed,
        "writer": mock_writer,
    }


@pytest.fixture
def mock_spark_functions():
    """Fixture mock các hàm current_timestamp, to_date, lit của pyspark.sql.functions."""
    with (
        patch("src.data_pipeline.bronze_layer.current_timestamp") as mock_ts,
        patch("src.data_pipeline.bronze_layer.to_date") as mock_to_date,
        patch("src.data_pipeline.bronze_layer.lit") as mock_lit,
    ):
        mock_ts.return_value = MagicMock(name="mock_current_timestamp")
        mock_to_date.return_value = MagicMock(name="mock_to_date")
        mock_lit.return_value = MagicMock(name="mock_lit")
        yield {
            "current_timestamp": mock_ts,
            "to_date": mock_to_date,
            "lit": mock_lit,
        }


def test_create_spark_session():
    """Kiểm tra create_spark_session gọi get_spark_session với appName đúng."""
    with patch("src.data_pipeline.bronze_layer.get_spark_session") as mock_get_spark:
        mock_session = MagicMock()
        mock_get_spark.return_value = mock_session

        session = create_spark_session()

        assert session == mock_session
        mock_get_spark.assert_called_once_with("BronzeLayer_MongoDB_Ingestion")


def test_ingest_bronze_layer_success(mock_env, mock_spark_pipeline, mock_spark_functions):
    """Kiểm tra pipeline ingest_bronze_layer đọc pymongo, convert ObjectId, tạo DataFrame và ghi Parquet."""
    mock_spark = mock_spark_pipeline["spark"]
    mock_df_raw = mock_spark_pipeline["df_raw"]
    mock_df_transformed = mock_spark_pipeline["df_transformed"]
    mock_writer = mock_spark_pipeline["writer"]

    sample_docs = [
        {"_id": MagicMock(__str__=lambda s: "60c72b2f9b1d8b2bad000001"), "list_id": 101, "title": "MacBook Pro M1"},
        {"_id": MagicMock(__str__=lambda s: "60c72b2f9b1d8b2bad000002"), "list_id": 102, "title": "Dell XPS 13"},
    ]

    mock_client = MagicMock()
    mock_client.__enter__.return_value = mock_client
    mock_coll = mock_client[mock_env["MONGO_DB"]][mock_env["MONGO_COLL_RAW"]]
    mock_coll.find.return_value = sample_docs

    with (
        patch("src.data_pipeline.bronze_layer.create_spark_session", return_value=mock_spark),
        patch("src.data_pipeline.bronze_layer.MongoClient", return_value=mock_client) as mock_mongo_cls,
    ):
        result_df = ingest_bronze_layer()

    # 1. Kiểm tra kết nối pymongo
    mock_mongo_cls.assert_called_once_with(mock_env["MONGO_URI"], serverSelectionTimeoutMS=10000)
    mock_coll.find.assert_called_once_with({})

    # 2. Kiểm tra nạp vào Spark DataFrame sau khi chuẩn hóa ObjectId
    assert sample_docs[0]["_id"] == "60c72b2f9b1d8b2bad000001"
    mock_spark.createDataFrame.assert_called_once_with(sample_docs)

    # 3. Kiểm tra các bước thêm metadata columns
    first_col_call = mock_df_raw.withColumn.call_args
    assert first_col_call[0][0] == "_ingested_at"
    assert first_col_call[0][1] == mock_spark_functions["current_timestamp"].return_value

    second_col_call = mock_df_transformed.withColumn.call_args_list[0]
    assert second_col_call[0][0] == "_ingested_date"
    assert second_col_call[0][1] == mock_spark_functions["to_date"].return_value

    third_col_call = mock_df_transformed.withColumn.call_args_list[1]
    assert third_col_call[0][0] == "_source"
    assert third_col_call[0][1] == mock_spark_functions["lit"].return_value

    # 4. Kiểm tra ghi Parquet
    mock_df_transformed.write.format.assert_called_once_with("parquet")
    mock_writer.mode.assert_called_once_with("append")
    mock_writer.partitionBy.assert_called_once_with("_ingested_date")
    mock_writer.save.assert_called_once_with(mock_env["DATALAKE_BRONZE_DIR"])

    # 5. Kiểm tra đóng SparkSession vì là owner
    mock_spark.stop.assert_called_once()
    assert result_df == mock_df_transformed


def test_ingest_bronze_layer_empty_collection(mock_env, mock_spark_pipeline):
    """Kiểm tra khi MongoDB trả về 0 bản ghi, trả về DataFrame rỗng an toàn và không ghi Parquet."""
    mock_spark = mock_spark_pipeline["spark"]
    mock_empty_df = MagicMock(name="EmptyBronzeDataFrame")
    mock_spark.createDataFrame.return_value = mock_empty_df

    mock_client = MagicMock()
    mock_client.__enter__.return_value = mock_client
    mock_coll = mock_client[mock_env["MONGO_DB"]][mock_env["MONGO_COLL_RAW"]]
    mock_coll.find.return_value = []

    with (
        patch("src.data_pipeline.bronze_layer.create_spark_session", return_value=mock_spark),
        patch("src.data_pipeline.bronze_layer.MongoClient", return_value=mock_client),
        patch("src.data_pipeline.bronze_layer.logger.warning") as mock_warn,
    ):
        result_df = ingest_bronze_layer()

    mock_warn.assert_called_once()
    assert "MongoDB collection is empty" in mock_warn.call_args[0][0]
    assert result_df == mock_empty_df
    # Không gọi lệnh write khi rỗng
    mock_spark_pipeline["writer"].save.assert_not_called()
    mock_spark.stop.assert_called_once()


def test_ingest_bronze_layer_with_external_spark(mock_env, mock_spark_pipeline, mock_spark_functions):
    """Kiểm tra khi truyền spark từ ngoài vào (qua Orchestrator), không tự tiện gọi spark.stop()."""
    mock_spark = mock_spark_pipeline["spark"]

    mock_client = MagicMock()
    mock_client.__enter__.return_value = mock_client
    mock_coll = mock_client[mock_env["MONGO_DB"]][mock_env["MONGO_COLL_RAW"]]
    mock_coll.find.return_value = [{"list_id": 999}]

    with patch("src.data_pipeline.bronze_layer.MongoClient", return_value=mock_client):
        ingest_bronze_layer(spark=mock_spark)

    # Đảm bảo KHÔNG đóng Spark vì không phải là owner
    mock_spark.stop.assert_not_called()


def test_ingest_bronze_layer_read_failure(mock_env, mock_spark_pipeline):
    """Kiểm tra xử lý lỗi khi kết nối MongoDB thất bại: log lỗi, ném ngoại lệ và vẫn gọi spark.stop()."""
    mock_spark = mock_spark_pipeline["spark"]

    mock_client = MagicMock()
    mock_client.__enter__.return_value = mock_client
    mock_coll = mock_client[mock_env["MONGO_DB"]][mock_env["MONGO_COLL_RAW"]]
    mock_coll.find.side_effect = ConnectionError("Could not connect to MongoDB server")

    with (
        patch("src.data_pipeline.bronze_layer.create_spark_session", return_value=mock_spark),
        patch("src.data_pipeline.bronze_layer.MongoClient", return_value=mock_client),
        patch("src.data_pipeline.bronze_layer.logger.error") as mock_logger_error,
    ):
        with pytest.raises(ConnectionError, match="Could not connect to MongoDB server"):
            ingest_bronze_layer()

        mock_logger_error.assert_called_once()
        assert "Error in ETL Bronze" in mock_logger_error.call_args[0][0]

    mock_spark.stop.assert_called_once()


def test_ingest_bronze_layer_write_failure(mock_env, mock_spark_pipeline, mock_spark_functions):
    """Kiểm tra xử lý lỗi khi ghi Parquet thất bại: log lỗi, ném ngoại lệ và vẫn đóng Spark."""
    mock_spark = mock_spark_pipeline["spark"]
    mock_writer = mock_spark_pipeline["writer"]
    mock_writer.save.side_effect = PermissionError("Permission denied writing to Datalake")

    mock_client = MagicMock()
    mock_client.__enter__.return_value = mock_client
    mock_coll = mock_client[mock_env["MONGO_DB"]][mock_env["MONGO_COLL_RAW"]]
    mock_coll.find.return_value = [{"list_id": 1}]

    with (
        patch("src.data_pipeline.bronze_layer.create_spark_session", return_value=mock_spark),
        patch("src.data_pipeline.bronze_layer.MongoClient", return_value=mock_client),
        patch("src.data_pipeline.bronze_layer.logger.error") as mock_logger_error,
    ):
        with pytest.raises(PermissionError, match="Permission denied writing to Datalake"):
            ingest_bronze_layer()

        mock_logger_error.assert_called_once()
        assert "Error in ETL Bronze" in mock_logger_error.call_args[0][0]

    mock_spark.stop.assert_called_once()


def test_ingest_bronze_layer_custom_env_paths(monkeypatch, mock_spark_pipeline, mock_spark_functions):
    """Kiểm tra pipeline áp dụng đúng URI và output path từ các biến môi trường tùy chỉnh."""
    custom_env = {
        "MONGO_URI": "mongodb://test-host:27017",
        "MONGO_DB": "prod_warehouse",
        "MONGO_COLL_RAW": "chotot_crawler_v2",
        "DATALAKE_BRONZE_DIR": "s3a://my-lake-bucket/bronze/laptops",
    }
    for k, v in custom_env.items():
        monkeypatch.setenv(k, v)

    mock_spark = mock_spark_pipeline["spark"]
    mock_writer = mock_spark_pipeline["writer"]

    mock_client = MagicMock()
    mock_client.__enter__.return_value = mock_client
    mock_coll = mock_client["prod_warehouse"]["chotot_crawler_v2"]
    mock_coll.find.return_value = [{"list_id": 888}]

    with (
        patch("src.data_pipeline.bronze_layer.create_spark_session", return_value=mock_spark),
        patch("src.data_pipeline.bronze_layer.MongoClient", return_value=mock_client) as mock_mongo_cls,
    ):
        ingest_bronze_layer()

    mock_mongo_cls.assert_called_once_with("mongodb://test-host:27017", serverSelectionTimeoutMS=10000)
    mock_writer.save.assert_called_once_with("s3a://my-lake-bucket/bronze/laptops")
    mock_spark.stop.assert_called_once()


def test_sanitize_documents_mixed_numeric_and_types():
    """Kiểm tra _sanitize_documents chuẩn hóa int -> float khi có float, giữ nguyên boolean và convert ObjectId."""
    docs = [
        {"_id": 12345, "seller_rating": 5, "screen": 14, "is_pro": True},
        {"_id": 67890, "seller_rating": 4.8, "screen": 15.6, "is_pro": False},
        {"_id": "str_id", "seller_rating": None, "screen": None, "is_pro": None},
    ]

    res = _sanitize_documents(docs)

    # 1. _id luôn là string
    assert res[0]["_id"] == "12345"
    assert res[1]["_id"] == "67890"
    assert res[2]["_id"] == "str_id"

    # 2. Xung đột LongType vs DoubleType được giải quyết: int -> float
    assert isinstance(res[0]["seller_rating"], float)
    assert res[0]["seller_rating"] == 5.0
    assert res[1]["seller_rating"] == 4.8

    assert isinstance(res[0]["screen"], float)
    assert res[0]["screen"] == 14.0
    assert res[1]["screen"] == 15.6

    # 3. Boolean không bị biến thành float
    assert isinstance(res[0]["is_pro"], bool)
    assert res[0]["is_pro"] is True
    assert isinstance(res[1]["is_pro"], bool)
    assert res[1]["is_pro"] is False


def test_sanitize_documents_mixed_str_and_numeric():
    """Kiểm tra _sanitize_documents ép về string nếu một trường vừa có chuỗi vừa có số."""
    docs = [
        {"ram": "8GB", "price": 10000000},
        {"ram": 16, "price": "15000000"},
    ]

    res = _sanitize_documents(docs)

    assert res[0]["ram"] == "8GB"
    assert res[1]["ram"] == "16"
    assert res[0]["price"] == "10000000"
    assert res[1]["price"] == "15000000"


def test_sanitize_documents_empty_list():
    """Kiểm tra xử lý an toàn danh sách rỗng."""
    assert _sanitize_documents([]) == []

