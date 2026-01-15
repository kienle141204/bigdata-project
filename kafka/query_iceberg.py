"""Script to query the Silver Iceberg table on S3."""
import os
import sys
from pyspark.sql import SparkSession
from loguru import logger

# Add parent directory to path to import local modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import S3_CONFIG

def query_silver():
    ICEBERG_VERSION = "1.5.0"
    ICEBERG_PKG = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}"
    AWS_PKG = "org.apache.hadoop#hadoop-aws:3.3.4"
    BUNDLE_PKG = "com.amazonaws#aws-java-sdk-bundle:1.12.262"

    warehouse_path = f"s3a://{S3_CONFIG['bucket_name']}/{S3_CONFIG['prefix']}/iceberg_warehouse"

    spark = SparkSession.builder \
        .appName("QuerySilverIceberg") \
        .config("spark.jars.packages", f"{ICEBERG_PKG},{AWS_PKG},{BUNDLE_PKG}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
        .config("spark.sql.catalog.iceberg_catalog.warehouse", warehouse_path) \
        .config("spark.hadoop.fs.s3a.access.key", S3_CONFIG['aws_access_key_id']) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_CONFIG['aws_secret_access_key']) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{S3_CONFIG['region_name']}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .master("local[*]") \
        .getOrCreate()

    table_name = "iceberg_catalog.silver.matches"
    
    try:
        logger.info(f"Checking table: {table_name}")
        df = spark.read.table(table_name)
        print(f"\n--- BẢNG SILVER (ICEBERG) ---")
        print(f"Tổng số bản ghi: {df.count()}")
        df.select("season", "matchweek", "home_team", "away_team", "home_score", "away_score").show(5)
    except Exception as e:
        print(f"\n[!] Bảng Silver chưa tồn tại hoặc bị lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    query_silver()
