"""
Spark Structured Streaming ETL
Real-time processing: Kafka → Silver (S3) directly
NO Bronze layer for streaming
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import S3_CONFIG


def create_spark_session():
    """Create Spark session for streaming with UI always available."""
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",  # Kafka connector
        "org.apache.hadoop:hadoop-aws:3.3.4"  # S3 support
    ]
    
    return SparkSession.builder \
        .appName("PremierLeagueStreamingETL") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .config("spark.ui.host", "0.0.0.0") \
        .config("spark.driver.host", "0.0.0.0") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.ui.retainedJobs", "1000") \
        .config("spark.ui.retainedStages", "1000") \
        .config("spark.ui.retainedTasks", "10000") \
        .config("spark.worker.ui.retainedExecutors", "1000") \
        .config("spark.worker.ui.retainedDrivers", "1000") \
        .config("spark.sql.ui.retainedExecutions", "1000") \
        .config("spark.streaming.ui.retainedBatches", "1000") \
        .config("spark.sql.streaming.checkpointLocation", 
                f"s3a://{S3_CONFIG['bucket_name']}/checkpoints/streaming") \
        .config("spark.hadoop.fs.s3a.access.key", S3_CONFIG.get("aws_access_key_id", "")) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_CONFIG.get("aws_secret_access_key", "")) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{S3_CONFIG.get('region_name', 'us-east-1')}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def define_match_schema():
    """Define schema for match data from Kafka."""
    return StructType([
        StructField("match_id", IntegerType(), False),
        StructField("season", StringType(), False),
        StructField("matchweek", IntegerType(), False),
        StructField("url", StringType(), True),
        StructField("scraped_at", StringType(), True),
        StructField("match_info", StructType([
            StructField("home_team", StringType(), True),
            StructField("away_team", StringType(), True),
            StructField("home_score", IntegerType(), True),
            StructField("away_score", IntegerType(), True),
        ]), True),
        StructField("lineups", StructType([
            StructField("home", MapType(StringType(), StringType()), True),
            StructField("away", MapType(StringType(), StringType()), True),
        ]), True),
        StructField("events", MapType(StringType(), StringType()), True),
        StructField("statistics", MapType(StringType(), MapType(StringType(), StringType())), True),
    ])


def flatten_match_data(df):
    """
    Flatten and transform match data for Silver layer.
    Similar to Bronze→Silver transformation but inline.
    """
    return df.select(
        col("match_id"),
        col("season"),
        col("matchweek"),
        col("url"),
        col("scraped_at"),
        
        # Match info
        col("match_info.home_team").alias("home_team"),
        col("match_info.away_team").alias("away_team"),
        col("match_info.home_score").alias("home_score"),
        col("match_info.away_score").alias("away_score"),
        
        # Add processing timestamp
        current_timestamp().alias("processed_at"),
        lit("streaming").alias("source_type")
    )


def start_streaming_etl(kafka_servers="kafka:9093", topic="raw-match-data"):
    """
    Start Spark Structured Streaming job.
    
    Args:
        kafka_servers: Kafka bootstrap servers
        topic: Kafka topic to consume
    """
    logger.info("🚀 Starting Spark Structured Streaming ETL")
    logger.info(f"   Kafka: {kafka_servers}")
    logger.info(f"   Topic: {topic}")
    logger.info(f"   S3 Bucket: {S3_CONFIG['bucket_name']}")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✅ Spark session created")
    
    # Force UI initialization with a dummy action
    logger.info("🎯 Initializing Spark UI...")
    try:
        # Create a simple dummy DataFrame to activate the UI
        dummy_df = spark.createDataFrame([(1, "init")], ["id", "status"])
        dummy_df.count()  # Execute action to trigger UI
        logger.success("✅ Spark UI initialized and accessible at: http://localhost:4040")
    except Exception as e:
        logger.warning(f"⚠️  UI init warning (ignorable): {e}")
    
    # Define schema
    schema = define_match_schema()
    
    # Read from Kafka
    logger.info("📡 Connecting to Kafka stream...")
    
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("✅ Connected to Kafka stream")
    
    # Parse JSON from Kafka value
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Flatten and transform
    logger.info("🔄 Applying transformations...")
    transformed_df = flatten_match_data(parsed_df)
    
    # Write to Silver (S3) as Parquet
    output_path = f"s3a://{S3_CONFIG['bucket_name']}/{S3_CONFIG.get('prefix', 'premier_league')}/silver/streaming/"
    checkpoint_path = f"s3a://{S3_CONFIG['bucket_name']}/{S3_CONFIG.get('prefix', 'premier_league')}/checkpoints/streaming/"
    
    logger.info(f"📊 Writing to Silver: {output_path}")
    
    query = transformed_df \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("season", "matchweek") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.success("✅ Streaming query started!")
    logger.info("🎯 Spark UI available at: http://localhost:4040")
    logger.info("⏸️  Press Ctrl+C to stop")
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.warning("🛑 Stopping streaming query...")
        query.stop()
        logger.info("✅ Streaming stopped")


def start_streaming_to_csv():
    """
    Alternative: Write as CSV instead of Parquet for consistency with batch.
    """
    logger.info("🚀 Starting Streaming ETL (CSV output)")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    schema = define_match_schema()
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")) \
        .option("subscribe", "raw-match-data") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse and transform
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    transformed_df = flatten_match_data(parsed_df)
    
    # Write as CSV
    output_path = f"s3a://{S3_CONFIG['bucket_name']}/{S3_CONFIG.get('prefix', 'premier_league')}/silver/streaming/"
    checkpoint_path = f"s3a://{S3_CONFIG['bucket_name']}/checkpoints/streaming-csv/"
    
    query = transformed_df \
        .writeStream \
        .format("csv") \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .option("header", "true") \
        .partitionBy("season", "matchweek") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.success("✅ Streaming query started (CSV output)!")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Spark Streaming ETL")
    parser.add_argument("--kafka-servers", default="kafka:9093", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="raw-match-data", help="Kafka topic")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet", help="Output format")
    
    args = parser.parse_args()
    
    if args.format == "csv":
        start_streaming_to_csv()
    else:
        start_streaming_etl(args.kafka_servers, args.topic)
