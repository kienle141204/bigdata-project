"""Test S3 connectivity from Spark."""
import os
import sys
from pyspark.sql import SparkSession
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import S3_CONFIG

def test_s3():
    AWS_PKG = "org.apache.hadoop:hadoop-aws:3.3.4"
    BUNDLE_PKG = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

    spark = SparkSession.builder \
        .appName("S3Test") \
        .config("spark.jars.packages", f"{AWS_PKG},{BUNDLE_PKG}") \
        .config("spark.hadoop.fs.s3a.access.key", S3_CONFIG['aws_access_key_id']) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_CONFIG['aws_secret_access_key']) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{S3_CONFIG['region_name']}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .master("local[*]") \
        .getOrCreate()

    try:
        test_path = f"s3a://{S3_CONFIG['bucket_name']}/{S3_CONFIG['prefix']}/test_write.txt"
        logger.info(f"Testing write to: {test_path}")
        spark.sparkContext.parallelize(["Hello S3"]).saveAsTextFile(test_path)
        logger.info("Successfully wrote to S3!")
    except Exception as e:
        logger.error(f"S3 Write Failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    test_s3()
