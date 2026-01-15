import os
import sys
from loguru import logger
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.processor import S3DataStore
from config.settings import S3_CONFIG

def sync_gold_layer_csv():
    """
    Sync Iceberg Silver layer to Gold layer as CSV files.
    Creates 3 tables per team: matches.csv, players.csv, events.csv
    Matches the exact column structure of the old format.
    """
    logger.info("="*60)
    logger.info("SYNCING SILVER (ICEBERG) -> GOLD (CSV)")
    logger.info("="*60)
    
    # Initialize Spark with Iceberg support
    # Note: Using S3_CONFIG keys correctly
    warehouse_path = f"s3a://{S3_CONFIG['bucket_name']}/{S3_CONFIG['prefix']}/iceberg_warehouse"
    
    spark = SparkSession.builder \
        .appName("SilverToGoldCSV") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                                       "org.apache.hadoop:hadoop-aws:3.3.4,"
                                       "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
        .config("spark.sql.catalog.iceberg_catalog.warehouse", warehouse_path) \
        .config("spark.hadoop.fs.s3a.access.key", S3_CONFIG['aws_access_key_id']) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_CONFIG['aws_secret_access_key']) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{S3_CONFIG['region_name']}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("ERROR")
    
    store = S3DataStore()
    
    try:
        # 1. Read Iceberg tables
        logger.info(f"Checking Iceberg tables in {warehouse_path}...")
        
        tables = ["matches", "players", "events"]
        available_tables = {}
        
        for table in tables:
            full_table_name = f"iceberg_catalog.silver.{table}"
            if spark.catalog.tableExists(full_table_name):
                logger.info(f"Found table: {full_table_name}")
                available_tables[table] = spark.table(full_table_name)
            else:
                logger.warning(f"Table {full_table_name} does not exist.")
        
        if not available_tables:
            logger.error("No Silver tables found. Please run spark_consumer.py first.")
            return

        df_matches = available_tables.get("matches")
        df_players = available_tables.get("players")
        df_events = available_tables.get("events")
        
        # 2. Identify all unique teams
        teams = set()
        if df_matches:
            home_teams = [r[0] for r in df_matches.select("home_team").distinct().collect() if r[0]]
            away_teams = [r[0] for r in df_matches.select("away_team").distinct().collect() if r[0]]
            teams.update(home_teams)
            teams.update(away_teams)
        
        # Filter out invalid teams
        teams = {t for t in teams if t and str(t).strip() and str(t).lower() != 'none'}
        
        if not teams:
            logger.warning("No teams found in matches table.")
            return
            
        logger.info(f"Processing {len(teams)} teams...")
        
        for team in sorted(list(teams)):
            # Clean team name for folder (matches etl.py logic)
            team_folder = team.replace(" ", "_").replace("/", "_")
            logger.info(f"Processing: {team} -> gold_iceberg/{team_folder}/")
            
            # --- 1. Matches Table ---
            if df_matches:
                team_matches = df_matches.filter((col("home_team") == team) | (col("away_team") == team)) \
                    .orderBy("season", "matchweek")
                
                if team_matches.count() > 0:
                    pdf_matches = team_matches.toPandas()
                    s3_key = f"{S3_CONFIG['prefix']}/gold_iceberg/{team_folder}/matches.csv"
                    store.upload_csv(pdf_matches.to_dict('records'), layer="gold", s3_key=s3_key)
                    logger.info(f"  Saved matches.csv")
            
            # --- 2. Players Table ---
            if df_players:
                # IMPORTANT: Only include players for THIS team
                team_players = df_players.filter(col("team") == team) \
                    .orderBy("season", "matchweek", "player_name")
                
                if team_players.count() > 0:
                    pdf_players = team_players.toPandas()
                    # Standard columns: match_id, season, matchweek, team, player_name, position
                    player_cols = ["match_id", "season", "matchweek", "team", "player_name", "position"]
                    available_cols = [c for c in player_cols if c in pdf_players.columns]
                    pdf_players = pdf_players[available_cols]
                    
                    s3_key = f"{S3_CONFIG['prefix']}/gold_iceberg/{team_folder}/players.csv"
                    store.upload_csv(pdf_players.to_dict('records'), layer="gold", s3_key=s3_key)
                    logger.info(f"  Saved players.csv")
            
            # --- 3. Events Table ---
            if df_events:
                # IMPORTANT: Only include events for THIS team
                team_events = df_events.filter(col("team") == team) \
                    .orderBy("season", "matchweek", "minute")
                
                if team_events.count() > 0:
                    pdf_events = team_events.toPandas()
                    # Standard columns: match_id, season, matchweek, team, event_type, player, minute
                    event_cols = ["match_id", "season", "matchweek", "team", "event_type", "player", "minute"]
                    available_cols = [c for c in event_cols if c in pdf_events.columns]
                    pdf_events = pdf_events[available_cols]
                    
                    s3_key = f"{S3_CONFIG['prefix']}/gold_iceberg/{team_folder}/events.csv"
                    store.upload_csv(pdf_events.to_dict('records'), layer="gold", s3_key=s3_key)
                    logger.info(f"  Saved events.csv")

        logger.info("="*60)
        logger.info("SILVER TO GOLD CSV SYNC COMPLETE")
        logger.info(f"Target: s3://{S3_CONFIG['bucket_name']}/{S3_CONFIG['prefix']}/gold_iceberg/")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Failed to sync Gold layer: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        spark.stop()

if __name__ == "__main__":
    sync_gold_layer_csv()
