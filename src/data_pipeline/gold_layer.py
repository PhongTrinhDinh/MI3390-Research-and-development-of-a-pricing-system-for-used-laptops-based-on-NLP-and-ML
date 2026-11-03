import os
from src.utils.logger import get_logger
from src.utils.spark_utils import get_spark_session
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    lower,
    trim,
    regexp_extract,
    log1p,
)
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    Imputer,
)

logger = get_logger(__name__, log_file="logs/etl_pipeline/gold.log")


def _safe_cast_float(str_col):
    """Ép kiểu chuỗi sang float an toàn, trả về NULL nếu chuỗi rỗng hoặc NULL."""
    return when((str_col.isNotNull()) & (trim(str_col) != ""), str_col.cast("float")).otherwise(lit(None))


def process_gold_layer(spark: SparkSession = None):
    is_owner = False
    if spark is None:
        spark = get_spark_session("GoldLayer_FeatureEngineering")
        is_owner = True

    silver_dir = os.getenv("DATALAKE_SILVER_DIR", "./data/processed/silver_layer")
    gold_dir = os.getenv("DATALAKE_GOLD_DIR", "./data/features/gold_layer")
    model_artifact_dir = os.getenv("MODEL_TRANSFORMER_DIR", "./models/transformers")

    logger.info(f"Reading data from Silver layer: {silver_dir}")

    try:
        df_silver = spark.read.parquet(silver_dir)

        # ----------------------------------------------------------------------
        # 1. Lọc bỏ Outliers & Dữ liệu rác của Biến mục tiêu (target_price)
        # ----------------------------------------------------------------------
        # Trên Chợ Tốt có rất nhiều tin đăng giá ảo: 0đ, 1.000đ (thương lượng), hoặc 999 triệu.
        # Lọc trong khoảng giá thực tế cho laptop: từ 500.000 VNĐ đến 200.000.000 VNĐ.
        df_filtered = df_silver.filter(
            col("target_price").isNotNull()
            & (col("target_price") >= 500000.0)
            & (col("target_price") <= 200000000.0)
        )

        # ----------------------------------------------------------------------
        # 2. Khôi phục các đặc trưng phần cứng cốt lõi: RAM & Ổ cứng (Storage)
        # ----------------------------------------------------------------------
        # Trích xuất dung lượng RAM (GB) an toàn
        ram_str = regexp_extract(lower(col("raw_ram")), r"(\d+)\s*gb", 1)
        ram_from_raw = _safe_cast_float(ram_str)
        df_hw = df_filtered.withColumn("ram_gb", ram_from_raw)

        # Trích xuất dung lượng Ổ cứng (GB): quy đổi TB -> GB (1 TB = 1024 GB)
        storage_tb_str = regexp_extract(lower(col("raw_hard_drive")), r"(\d+(?:\.\d+)?)\s*tb", 1)
        storage_tb = _safe_cast_float(storage_tb_str) * 1024.0

        storage_gb_str = regexp_extract(lower(col("raw_hard_drive")), r"(\d+)\s*gb", 1)
        storage_gb = _safe_cast_float(storage_gb_str)

        df_hw = df_hw.withColumn(
            "storage_gb",
            when((storage_tb.isNotNull()) & (storage_tb > 0), storage_tb).otherwise(storage_gb)
        ).withColumn(
            "storage_type",
            when(lower(col("raw_hard_drive_type")).contains("ssd"), lit("ssd"))
            .when(lower(col("raw_hard_drive_type")).contains("hdd"), lit("hdd"))
            .otherwise(lit("unknown"))
        )

        # ----------------------------------------------------------------------
        # 3. Lọc bỏ các cột Data Lineage (_src) và Xử lý Missing Values
        # ----------------------------------------------------------------------
        # - Loại bỏ các cột _src (Data Lineage không dùng trực tiếp làm feature huấn luyện)
        # - has_warranty & has_bundle: Đã được chuẩn hóa nhị phân tinh gọn từ Silver Layer
        # - is_like_new, is_upgraded, seller_sold_count: Điền mặc định 0 nếu thiếu
        cols_to_drop = [c for c in df_hw.columns if c.endswith("_src")]
        df_preprocessed = df_hw.drop(*cols_to_drop).fillna({
            "has_warranty": 0,
            "has_bundle": 0,
            "is_like_new": 0,
            "is_upgraded": 0,
            "seller_sold_count": 0,
        })

        # ----------------------------------------------------------------------
        # 4. Phân nhóm Đặc trưng (Features Selection theo EDA)
        # ----------------------------------------------------------------------
        # A. Nhóm đặc trưng cốt lõi (Must-have features) & Bổ trợ giá trị (Value-add features):
        #    - Numeric: ram_gb (0.739), storage_gb (0.687), cpu_gen (0.612), screen_size_inch (0.283),
        #               seller_sold_count (0.166), year_production
        #    - Categorical: brand_name, product_line, cpu_vendor, cpu_model, gpu_type, gpu_model,
        #                   storage_type, resolution, origin_type, repair_status, region_name
        #    - Binary: has_warranty, has_bundle, is_like_new, is_upgraded
        #
        # B. Đã loại bỏ các biến thiếu cực đoan (>95% missing) & tương quan gần 0 (r ≈ 0):
        #    - Thiếu cực đoan: year_purchase (99.8%), battery_cycles/hours/pct (>99%), condition_pct (97.8%),
        #                      origin_country (95.2%), is_new_seal (98.8%), battery_condition
        #    - Tương quan r ≈ 0: seller_rating (0.002), is_seller_rating_missing, touch_sensitive (0.043)
        #    - Trùng lặp/Đã gom nhóm: warranty_months, warranty_type -> has_warranty; bundles/gift_* -> has_bundle

        categorical_cols = [
            "brand_name",
            "product_line",
            "cpu_vendor",
            "cpu_model",
            "gpu_type",
            "gpu_model",
            "storage_type",
            "resolution",
            "origin_type",
            "repair_status",
            "region_name",
        ]
        df_clean = df_preprocessed.fillna({c: "unknown" for c in categorical_cols})

        binary_cols = [
            "has_warranty",
            "has_bundle",
            "is_like_new",
            "is_upgraded",
        ]
        for c in binary_cols:
            df_clean = df_clean.withColumn(c, col(c).cast("float"))

        numeric_cols = [
            "ram_gb",
            "storage_gb",
            "cpu_gen",
            "screen_size_inch",
            "seller_sold_count",
            "year_production",
        ]
        for c in numeric_cols:
            df_clean = df_clean.withColumn(c, col(c).cast("float"))

        # ----------------------------------------------------------------------
        # 5. Xây dựng ML Pipeline Transformers (Imputer, Indexer, OHE, Assembler)
        # ----------------------------------------------------------------------
        stages = []

        # 5.1. Imputer cho các giá trị số còn thiếu (Median strategy)
        imputer = Imputer(
            inputCols=numeric_cols,
            outputCols=[f"{c}_imputed" for c in numeric_cols]
        ).setStrategy("median")
        stages.append(imputer)

        # 5.2. StringIndexer + OneHotEncoder cho các biến phân loại
        indexer = StringIndexer(
            inputCols=categorical_cols,
            outputCols=[f"{c}_idx" for c in categorical_cols],
            handleInvalid="keep"
        )
        encoder = OneHotEncoder(
            inputCols=[f"{c}_idx" for c in categorical_cols],
            outputCols=[f"{c}_ohe" for c in categorical_cols],
            handleInvalid="keep"
        )
        stages.extend([indexer, encoder])

        # 5.3. VectorAssembler gom toàn bộ đặc trưng vào cột `features`
        # Sử dụng handleInvalid="keep" để không âm thầm drop bất kỳ dòng dữ liệu nào
        assembler_inputs = (
            binary_cols
            + [f"{c}_imputed" for c in numeric_cols]
            + [f"{c}_ohe" for c in categorical_cols]
        )
        assembler = VectorAssembler(
            inputCols=assembler_inputs,
            outputCol="features",
            handleInvalid="keep"
        )
        stages.append(assembler)

        # ----------------------------------------------------------------------
        # 6. Fit Pipeline, Transform & Lưu Trữ Artifacts
        # ----------------------------------------------------------------------
        pipeline = Pipeline(stages=stages)
        pipeline_model = pipeline.fit(df_clean)
        df_gold = pipeline_model.transform(df_clean)

        # Tạo biến mục tiêu chuẩn hóa Log-Price song song với target_price
        df_gold = df_gold.withColumn("log_price", log1p(col("target_price")))

        df_gold_output = df_gold.select("post_id", "features", "target_price", "log_price")

        logger.info(f"Saving Gold features Parquet to: {gold_dir}")
        os.makedirs(gold_dir, exist_ok=True)
        df_gold_output.write.format("parquet") \
            .option("compression", "snappy") \
            .mode("overwrite") \
            .save(gold_dir)

        logger.info(f"Saving PipelineModel artifact to: {model_artifact_dir}")
        os.makedirs(model_artifact_dir, exist_ok=True)
        pipeline_model.write().overwrite().save(model_artifact_dir)

        logger.info("Successfully finished Gold layer Feature Engineering pipeline.")
        return df_gold_output, pipeline_model

    except Exception as e:
        logger.error(f"Error in Gold layer Feature Engineering: {e}")
        raise
    finally:
        if is_owner:
            logger.info("Stopping SparkSession...")
            spark.stop()


if __name__ == "__main__":
    process_gold_layer()
