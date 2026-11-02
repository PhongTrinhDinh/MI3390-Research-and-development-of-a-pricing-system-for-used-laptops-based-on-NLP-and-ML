import os
import sys
from pyspark.sql import SparkSession


def setup_spark_env():
    """
    Chuẩn hóa toàn bộ biến môi trường phục vụ PySpark trên Windows:
    1. Trỏ PYSPARK_PYTHON và PYSPARK_DRIVER_PYTHON trực tiếp về python.exe của .venv
       (khắc phục triệt để lỗi App Execution Aliases của Windows Store).
    2. Chuẩn hóa JAVA_HOME (loại bỏ đuôi \\bin hoặc /bin nếu có).
    3. Tự động nhận diện HADOOP_HOME nếu thư mục C:\\hadoop tồn tại.
    """
    # 1. Trỏ trực tiếp Python worker và driver về môi trường ảo hiện tại
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # 2. Chuẩn hóa JAVA_HOME
    java_home = os.environ.get("JAVA_HOME")
    if java_home and java_home.rstrip("\\/").endswith("bin"):
        os.environ["JAVA_HOME"] = os.path.dirname(java_home.rstrip("\\/"))

    # 3. Tự động liên kết HADOOP_HOME nếu có thư mục C:\hadoop
    if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
        os.environ["HADOOP_HOME"] = r"C:\hadoop"


def get_spark_session(app_name: str = "LaptopPricingPipeline") -> SparkSession:
    """
    Factory khởi tạo hoặc tái sử dụng SparkSession chuẩn hóa cho toàn bộ hệ thống
    (Bronze, Silver, Gold, Main ETL Orchestrator và Notebooks).
    """
    setup_spark_env()
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

