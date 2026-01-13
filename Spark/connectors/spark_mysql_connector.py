import os
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import MYSQL_CONFIG

load_dotenv()


def create_spark_session_with_mysql(app_name: str = "MySQLSparkApp", master: str = "local[*]", include_s3: bool = False) -> SparkSession:
    conf = SparkConf()
    
    conf.set("spark.app.name", app_name)
    conf.set("spark.master", master)
    
    mysql_host = MYSQL_CONFIG["host"]
    mysql_port = MYSQL_CONFIG["port"]
    mysql_user = MYSQL_CONFIG["user"]
    mysql_password = MYSQL_CONFIG["password"]
    mysql_database = MYSQL_CONFIG["database"]
    mysql_ssl_mode = MYSQL_CONFIG["ssl_mode"]
    
    if not mysql_password:
        raise ValueError("MYSQL_PASSWORD phải được thiết lập trong file .env")
    
    jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}?rewriteBatchedStatements=true"
    
    if mysql_ssl_mode == "REQUIRED":
        jdbc_url += "&useSSL=true&requireSSL=true"
        if MYSQL_CONFIG.get("ssl_ca"):
            jdbc_url += f"&trustCertificateKeyStoreUrl=file:{MYSQL_CONFIG['ssl_ca']}"
    
    conf.set("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
    
    packages = ["com.mysql:mysql-connector-j:8.2.0"]
    
    if include_s3 or app_name.endswith("Streaming"):
        packages.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    
    if include_s3:
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION", "ap-southeast-1")
        
        if aws_access_key_id and aws_secret_access_key:
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
            
            packages.append("org.apache.hadoop:hadoop-aws:3.3.4")
            packages.append("com.amazonaws:aws-java-sdk-bundle:1.12.262")
    
    conf.set("spark.jars.packages", ",".join(packages))
    conf.set("spark.jars.ivy", os.path.join(os.path.expanduser("~"), ".ivy2", "cache"))
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark, jdbc_url, {
        "user": mysql_user,
        "password": mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }


def read_mysql_table(spark: SparkSession, table_name: str, jdbc_url: str, connection_properties: dict):
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=connection_properties
    )
    return df


def write_mysql_table(df, jdbc_url: str, table_name: str, connection_properties: dict, mode: str = "overwrite"):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("batchsize", "5000") \
        .option("isolationLevel", "NONE") \
        .option("numPartitions", "4") \
        .mode(mode) \
        .options(**connection_properties) \
        .save()


if __name__ == "__main__":
    try:
        spark, jdbc_url, conn_props = create_spark_session_with_mysql(app_name="MySQLConnectionTest")
        spark.stop()
    except Exception as e:
        raise

