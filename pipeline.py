"""
Pipeline chính để xử lý dữ liệu Premier League.

Workflow:
1. Scrape dữ liệu -> Bronze (JSON)
2. Bronze -> Silver (CSV Tables)
3. Silver -> Gold (Team-specific tables)
"""

import argparse
import sys
import os
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.etl import ETLPipeline


def main():
    parser = argparse.ArgumentParser(description="Premier League Data Pipeline")
    
    # Options
    parser.add_argument("--seasons", type=str, nargs="+", 
                       help="List of Seasons (e.g., 2011/12 2012/13)")
    parser.add_argument("--all-seasons", action="store_true",
                       help="Process all seasons in Bronze layer")
    parser.add_argument("--create-gold", action="store_true",
                       help="Create Gold layer after Silver processing")
    parser.add_argument("--gold-only", action="store_true",
                       help="Only create Gold layer (skip Silver processing)")
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    logger.add(sys.stdout, 
              format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}", 
              level="INFO")
    logger.add("logs/pipeline_{time:YYYY-MM-DD}.log", rotation="1 day", level="DEBUG")
    
    # Initialize ETL
    etl = ETLPipeline()
    
    # Gold-only mode
    if args.gold_only:
        logger.info("Running in GOLD-ONLY mode")
        etl.silver_to_gold()
        return
    
    # Determine seasons to process
    seasons = None
    if not args.all_seasons and args.seasons:
        seasons = args.seasons
    # else: seasons = None -> auto discover
    
    # Run Bronze -> Silver conversion
    etl.bronze_to_silver(seasons=seasons)
    
    # Create Gold layer if requested
    if args.create_gold:
        logger.info("\nCreating Gold Layer...")
        etl.silver_to_gold()


if __name__ == "__main__":
    main()
