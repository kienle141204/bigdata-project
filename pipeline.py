import argparse
import sys
import os
from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scrape_to_s3 import S3ScraperApp
from data.etl import ETLPipeline

def run_pipeline(matchweek: int, season: str = "2025/26", skip_scrape: bool = False):
    """
    Execute the full data pipeline:
    1. Scrape (Source -> Bronze)
    2. Clean (Bronze -> Silver)
    3. Transform (Silver -> Gold)
    """
    logger.info("="*60)
    logger.info(f"STARTING PIPELINE FOR SEASON {season}, MW {matchweek}")
    logger.info("="*60)

    # Step 1: Ingestion (Scrape -> Bronze)
    if not skip_scrape:
        logger.info("[STEP 1/3] INGESTION: SCRAPING TO BRONZE")
        # We assume headless for pipeline execution
        with S3ScraperApp(headless=True) as app:
            results = app.scrape_and_upload_matchweek(matchweek, season)
            if not results:
                logger.error("Scraping failed or no data found. Aborting pipeline.")
                return
            logger.info(f"Successfully scraped {len(results)} matches to Bronze.")
    else:
        logger.info("[STEP 1/3] INGESTION: SKIPPED (Using existing Bronze data)")

    # Step 2: Cleaning (Bronze -> Silver)
    logger.info("[STEP 2/3] CLEANING: BRONZE TO SILVER")
    etl = ETLPipeline()
    try:
        etl.process_bronze_to_silver(season, matchweek)
    except Exception as e:
        logger.error(f"Bronze -> Silver failed: {e}")
        # We might continue or stop depending on severity. Stopping for now.
        return

    # Step 3: Transformation (Silver -> Gold)
    logger.info("[STEP 3/3] TRANSFORMATION: SILVER TO GOLD")
    try:
        etl.process_silver_to_gold(season, matchweek)
    except Exception as e:
        logger.error(f"Silver -> Gold failed: {e}")
        return

    logger.info("="*60)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("="*60)

def main():
    parser = argparse.ArgumentParser(description="Premier League Data Pipeline")
    parser.add_argument("--matchweeks", type=int, nargs="+", help="List of Matchweek numbers (e.g. 1 2 3)")
    parser.add_argument("--all-matchweeks", action="store_true", help="Process all 38 matchweeks")
    parser.add_argument("--seasons", type=str, nargs="+", default=["2025/26"], help="List of Seasons (e.g. 2024/25 2025/26)")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scraping and process existing data")
    
    args = parser.parse_args()
    
    if args.all_matchweeks:
        args.matchweeks = list(range(1, 39))
    elif not args.matchweeks:
        parser.error("Either --matchweeks or --all-matchweeks must be provided.")
    
    # Configure logging
    logger.remove()
    logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}", level="INFO")
    logger.add("logs/pipeline_{time:YYYY-MM-DD}.log", rotation="1 day", level="DEBUG")
    
    # Run core pipeline
    etl = None
    for season in args.seasons:
        for mw in args.matchweeks:
            # We initialize ETL once inside run_pipeline usually, but let's just run it.
            run_pipeline(mw, season, args.skip_scrape)

    # After all processing is done, create global masters
    logger.info("Generating Global Master Datasets...")
    if not etl:
        etl = ETLPipeline()
    etl.create_global_aggregates(args.seasons)

if __name__ == "__main__":
    main()
