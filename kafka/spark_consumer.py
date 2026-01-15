"""Spark Structured Streaming Consumer with 3 Iceberg Tables for Silver Layer on S3."""
import os
import sys
import json
from loguru import logger

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, MapType
except ImportError:
    logger.error("PySpark not found. Please install it.")
    sys.exit(1)

# Add parent directory to path to import local modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import KAFKA_CONFIG, S3_CONFIG
from data.etl import ETLPipeline

def get_spark_session():
    """Initialize Spark Session with Iceberg Support on S3."""
    ICEBERG_VERSION = "1.5.0"
    KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ICEBERG_PKG = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}"
    AWS_PKG = "org.apache.hadoop:hadoop-aws:3.3.4"
    BUNDLE_PKG = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

    warehouse_path = f"s3a://{S3_CONFIG['bucket_name']}/{S3_CONFIG['prefix']}/iceberg_warehouse"

    spark = SparkSession.builder \
        .appName("PremierLeagueIceberg3TablesStreaming") \
        .config("spark.jars.packages", f"{KAFKA_PKG},{ICEBERG_PKG},{AWS_PKG},{BUNDLE_PKG}") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.cores", "2") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
        .config("spark.sql.catalog.iceberg_catalog.warehouse", warehouse_path) \
        .config("spark.hadoop.fs.s3a.access.key", S3_CONFIG['aws_access_key_id']) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_CONFIG['aws_secret_access_key']) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{S3_CONFIG['region_name']}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/app/checkpoint_iceberg_s3") \
        .master("local[2]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def process_batch_iceberg(df, batch_id):
    """Process each batch and write to 3 Iceberg tables: matches, players, events."""
    count = df.count()
    if count == 0:
        return

    logger.info(f"Processing batch {batch_id} with {count} records")
    
    # Collect records
    records = [json.loads(row.value) for row in df.select("value").collect()]
    etl = ETLPipeline()
    
    flattened_matches = []
    flattened_players = []
    flattened_events = []
    
    for record in records:
        try:
            # 1. Matches
            m_flat = etl._flatten_match_data(record)
            if m_flat: flattened_matches.append(m_flat)
            
            # 2. Players
            p_flat = etl._extract_players_from_json(record)
            if p_flat: flattened_players.extend(p_flat)
            
            # 3. Events
            e_flat = etl._extract_events_from_json(record)
            if e_flat: flattened_events.extend(e_flat)
            
        except Exception as e:
            logger.error(f"Error processing record in batch: {e}")

    spark = df.sparkSession

    # Helper function to write to Iceberg
    def write_to_iceberg(data_list, table_name, partition_col=None):
        if not data_list: return
        json_rdd = spark.sparkContext.parallelize([json.dumps(r) for r in data_list])
        batch_df = spark.read.json(json_rdd)
        
        try:
            if not spark.catalog.tableExists(table_name):
                logger.info(f"Creating new Iceberg table: {table_name}")
                writer = batch_df.writeTo(table_name)
                if partition_col:
                    writer = writer.partitionedBy(partition_col)
                writer.create()
            else:
                batch_df.writeTo(table_name).append()
            logger.info(f"Updated {table_name}")
        except Exception as e:
            logger.error(f"Failed to write to {table_name}: {e}")

    # Write all 3 tables
    write_to_iceberg(flattened_matches, "iceberg_catalog.silver.matches", "season")
    write_to_iceberg(flattened_players, "iceberg_catalog.silver.players", "season")
    write_to_iceberg(flattened_events, "iceberg_catalog.silver.events", "season")

def main():
    spark = get_spark_session()
    logger.info("Streaming started. Writing to silver.matches, silver.players, silver.events...")

    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"]) \
            .option("subscribe", KAFKA_CONFIG["topic_raw"]) \
            .option("startingOffsets", "earliest") \
            .load()

        query = df.selectExpr("CAST(value AS STRING)") \
            .writeStream \
            .foreachBatch(process_batch_iceberg) \
            .start()

        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Streaming stopped by user")
    except Exception as e:
        logger.error(f"Streaming error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
