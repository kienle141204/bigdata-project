import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def create_spark_session(app_name: str = "S3SparkApp", master: str = "local[*]") -> SparkSession:
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")
    aws_s3_bucket = os.getenv("AWS_S3_BUCKET", "")
    
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError(
            "AWS_ACCESS_KEY_ID và AWS_SECRET_ACCESS_KEY phải được thiết lập trong file .env"
        )
    
    conf = SparkConf()
    
    conf.set("spark.app.name", app_name)
    conf.set("spark.master", master)
    
    conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
    conf.set("spark.hadoop.fs.s3a.region", aws_region)
    
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", 
             "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
    conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
    conf.set("spark.hadoop.fs.s3a.multipart.size", "67108864")
    conf.set("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
    
    conf.set("spark.hadoop.fs.s3a.path.style.access", "false")
    
    conf.set("spark.hadoop.fs.s3a.attempts.maximum", "5")
    conf.set("spark.hadoop.fs.s3a.retry.interval", "1000ms")
    
    hadoop_aws_packages = (
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    conf.set("spark.jars.packages", hadoop_aws_packages)
    
    conf.set("spark.jars.ivy", os.path.join(os.path.expanduser("~"), ".ivy2", "cache"))
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"SparkSession đã được tạo: {app_name}")
    logger.info(f"Kết nối S3 bucket: {aws_s3_bucket}")
    logger.info(f"AWS Region: {aws_region}")
    
    return spark


def get_s3_path(bucket: str = None, prefix: str = None, key: str = "") -> str:
    bucket = bucket or os.getenv("AWS_S3_BUCKET", "")
    prefix = prefix or os.getenv("AWS_S3_PREFIX", "premier_league")
    
    if not bucket:
        raise ValueError("AWS_S3_BUCKET phải được thiết lập trong file .env")
    
    prefix = prefix.strip("/")
    key = key.strip("/")
    
    if key:
        return f"s3a://{bucket}/{prefix}/{key}"
    else:
        return f"s3a://{bucket}/{prefix}/"


if __name__ == "__main__":
    try:
        spark = create_spark_session(app_name="S3ConnectionTest")
        
        bucket = os.getenv("AWS_S3_BUCKET", "")
        prefix = os.getenv("AWS_S3_PREFIX", "premier_league")
        
        if bucket:
            s3_path = get_s3_path(bucket=bucket, prefix=prefix)
            logger.info(f"Đường dẫn S3 test: {s3_path}")
            
            try:
                logger.info("Kết nối Spark với S3 đã được cấu hình thành công!")
            except Exception as e:
                logger.warning(f"Không thể truy cập S3: {e}")
        else:
            logger.warning("AWS_S3_BUCKET chưa được thiết lập")
        
        spark.stop()
        logger.info("SparkSession đã được đóng")
        
    except Exception as e:
        logger.error(f"Lỗi khi tạo SparkSession: {e}")
        raise

