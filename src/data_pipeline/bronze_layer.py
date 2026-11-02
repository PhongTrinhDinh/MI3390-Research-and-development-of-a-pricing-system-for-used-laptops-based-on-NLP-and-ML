import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType

from src.utils.logger import get_logger
from src.utils.spark_utils import get_spark_session

# logging setup
logger = get_logger(__name__, log_file="logs/etl_pipeline/bronze.log")

# load .env variables
load_dotenv()

# Schema dự phòng cho trường hợp MongoDB rỗng
BRONZE_FALLBACK_SCHEMA = StructType([
    StructField("_id", StringType(), True),
    StructField("list_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", StringType(), True),
    StructField("cpu", StringType(), True),
    StructField("ram", StringType(), True),
    StructField("hard_drive", StringType(), True),
    StructField("hard_drive_type", StringType(), True),
    StructField("pc_brand", StringType(), True),
    StructField("pc_cpu", StringType(), True),
    StructField("pc_ram", StringType(), True),
    StructField("laptop_screen_size", StringType(), True),
    StructField("elt_condition", StringType(), True),
    StructField("elt_origin", StringType(), True),
    StructField("elt_warranty", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("ward_name", StringType(), True),
    StructField("seller_rating", StringType(), True),
    StructField("seller_sold_count", StringType(), True),
    StructField("crawled_at", StringType(), True),
    StructField("url", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingested_date", DateType(), True),
    StructField("_source", StringType(), True),
])


def _sanitize_documents(docs: list[dict]) -> list[dict]:
    """
    Chuẩn hóa kiểu dữ liệu giữa các documents NoSQL để tránh lỗi CANNOT_MERGE_TYPE của Spark:
    1. Chuyển _id (ObjectId) sang string.
    2. Nếu một trường vừa có float vừa có int -> ép int thành float (tránh conflict DoubleType vs LongType).
    3. Nếu một trường vừa có str vừa có kiểu số -> ép số thành str (tránh conflict StringType vs Number).
    """
    if not docs:
        return docs

    # Thu thập tất cả các kiểu dữ liệu thực tế xuất hiện ở mỗi trường
    types_by_key = {}
    for doc in docs:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        for k, v in doc.items():
            if v is not None:
                types_by_key.setdefault(k, set()).add(type(v))

    # Xử lý đồng nhất kiểu cho các trường bị xung đột kiểu
    for k, types in types_by_key.items():
        clean_types = {t for t in types if t is not type(None)}

        # 1. Nếu có string kèm các kiểu khác (int, float, bool) -> ép tất cả sang string
        if str in clean_types and len(clean_types) > 1:
            for doc in docs:
                if k in doc and doc[k] is not None:
                    doc[k] = str(doc[k])

        # 2. Nếu có float và int (không có str) -> ép int sang float
        elif float in clean_types and int in clean_types:
            for doc in docs:
                if k in doc and isinstance(doc[k], int) and not isinstance(doc[k], bool):
                    doc[k] = float(doc[k])

    return docs


def create_spark_session():
    """Khởi tạo SparkSession chuẩn cho tầng Bronze thông qua spark_utils."""
    return get_spark_session("BronzeLayer_MongoDB_Ingestion")


def ingest_bronze_layer(spark: SparkSession = None):
    """
    Thu thập dữ liệu thô từ MongoDB bằng pymongo, chuyển thành Spark DataFrame,
    bổ sung metadata và ghi ra định dạng Parquet phân vùng theo ngày.
    """
    is_owner = False
    if spark is None:
        spark = create_spark_session()
        is_owner = True

    # Cấu hình kết nối MongoDB
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGO_DB", "chotot_db")
    coll_name = os.getenv("MONGO_COLL_RAW", "raw_laptops")
    output_dir = os.getenv("DATALAKE_BRONZE_DIR", "./data/raw/bronze_layer")

    logger.info(f"Connecting to MongoDB via pymongo: {db_name}.{coll_name}")

    try:
        # 1. Đọc dữ liệu từ MongoDB bằng pymongo thuần
        with MongoClient(mongo_uri, serverSelectionTimeoutMS=10000) as client:
            db = client[db_name]
            coll = db[coll_name]
            docs = list(coll.find({}))

        logger.info(f"Fetched {len(docs)} documents successfully from MongoDB")

        # 2. Xử lý trường hợp không có bản ghi nào
        if not docs:
            logger.warning("MongoDB collection is empty. Returning empty Bronze DataFrame.")
            df_bronze = spark.createDataFrame([], schema=BRONZE_FALLBACK_SCHEMA)
            return df_bronze

        # 3. Chuẩn hóa BSON & đồng nhất kiểu dữ liệu (tránh lỗi CANNOT_MERGE_TYPE của Spark)
        docs = _sanitize_documents(docs)

        # 4. Nạp vào Spark DataFrame
        df_raw = spark.createDataFrame(docs)

        # 5. Thêm metadata ingestion
        df_bronze = (
            df_raw.withColumn("_ingested_at", current_timestamp())
            .withColumn("_ingested_date", to_date(current_timestamp()))
            .withColumn("_source", lit("mongo_scrapy"))
        )

        # 6. Ghi Parquet vào Data Lake phân vùng theo _ingested_date
        logger.info(f"Writing data in Bronze layer to {output_dir}")
        df_bronze.write.format("parquet").mode("append").partitionBy("_ingested_date").save(output_dir)
        logger.info("Successfully finished Bronze layer ingestion.")
        return df_bronze

    except Exception as e:
        logger.error(f"Error in ETL Bronze: {e}")
        raise
    finally:
        if is_owner and spark is not None:
            logger.info("Stopping Bronze SparkSession...")
            spark.stop()
