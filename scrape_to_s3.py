"""Premier League Scraper with S3 upload."""
import os
import sys
import json
import time
import argparse
import concurrent.futures
from typing import List, Optional
from datetime import datetime
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import S3_CONFIG
from scraper.season_scraper import SeasonScraper
from data.processor import S3DataStore, S3StatusTracker
from data.etl import ETLPipeline


def _setup_logging():
    """Configure logging."""
    os.makedirs("logs", exist_ok=True)
    logger.remove()
    logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}", level="INFO")
    logger.add("logs/scraper_{time:YYYY-MM-DD}.log", rotation="1 day", level="DEBUG")

# Setup global logging
_setup_logging()


class SingleThreadScraper:
    """Helper class to run a scraper in a single thread/process context."""
    def __init__(self, headless=True):
        self.scraper = SeasonScraper(headless=headless)
        self.s3_store = S3DataStore()
        self.tracker = S3StatusTracker(flow_name="batch")
        
    def __enter__(self):
        self.scraper.start()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.scraper.stop()

    def process_matchweek(self, matchweek: int, season: str, delay: float, formats: List[str]) -> List[dict]:
        """Scrape all matches for a given matchweek using 3-field decision logic."""
        results = []
        all_matches = []
        
        logger.info(f"MW{matchweek}: Syncing match statuses for {season} (Batch Flow)...")
        
        # 1. Check summary page to determine is_played (Field 2)
        matches_summary = self.scraper.get_matchweek_matches(matchweek, season)
        
        if not matches_summary:
            logger.warning(f"MW{matchweek}: No matches found on page. Falling back to ID calculation.")
            match_ids = self.scraper._calculate_match_ids_for_matchweek(matchweek, season)
            matches_summary = [{"match_id": mid, "is_fallback": True} for mid in match_ids]

        for i, match in enumerate(matches_summary):
            match_id = match.get("match_id")
            
            # Field 2: is_played (based on scores from web)
            is_played_on_web = False
            if match.get("is_fallback"):
                is_played_on_web = True # If fallback, we must visit to check
            else:
                hs = match.get("home_score")
                as_ = match.get("away_score")
                if hs is not None and as_ is not None:
                    is_played_on_web = True
            
            # Field 3: is_scraped (based on S3 manifest + season 25/26 logic)
            state = self.tracker.get_match_state(match_id, season)
            is_already_scraped = state.get("is_scraped", False)

            # --- DECISION LOGIC ---
            if not is_played_on_web:
                logger.info(f"MW{matchweek} | Match {match_id}: Skipping (Not played yet)")
                continue

            if is_already_scraped:
                logger.info(f"MW{matchweek} | Match {match_id}: Skipping (Already scraped on S3)")
                continue

            # --- SCRAPE ACTION ---
            try:
                logger.info(f"MW{matchweek} | Match {match_id}: Scrape Required -> Scraping detail...")
                match_data = self.scraper.scrape_match_with_matchweek(match_id, matchweek, season)
                
                if match_data:
                    # Double check if played inside detail page if we were in fallback
                    info = match_data.get("match_info", {})
                    detail_is_played = info.get("home_score") is not None and info.get("away_score") is not None
                    
                    if not detail_is_played:
                        logger.info(f"MW{matchweek} | Match {match_id}: Detail confirms not played. Skipping.")
                        continue

                    all_matches.append(match_data)
                    self.s3_store.upload_match(match_data, formats)
                    
                    # Update 3-field state on S3
                    self.tracker.update_match_status(match_id, is_played=True, is_scraped=True)
                    
                    results.append({
                        "match_id": match_id,
                        "home": info.get("home_team"),
                        "away": info.get("away_team")
                    })
            except Exception as e:
                logger.error(f"MW{matchweek} | Error processing {match_id}: {e}")
            
            if i < len(matches_summary) - 1:
                time.sleep(delay)

        # Upload Aggregate for this MW
        if all_matches:
            self.s3_store.upload_aggregate(all_matches, season, matchweek)
            logger.info(f"MW{matchweek}: Updated aggregate data on S3.")
            
        logger.info(f"MW{matchweek}: Completed. {len(results)} new matches scraped.")
        return results


def run_matchweek_task(matchweek: int, season: str, delay: float, formats: List[str], headless: bool):
    """Worker function to be run in a separate thread."""
    try:
        with SingleThreadScraper(headless=headless) as worker:
            return worker.process_matchweek(matchweek, season, delay, formats)
    except Exception as e:
        logger.critical(f"Critical error in thread for MW{matchweek}: {e}")
        return []


class S3ScraperApp:
    """
    Wrapper class for backward compatibility with pipeline.py.
    Provides single-threaded scraping functionality.
    """
    def __init__(self, headless: bool = True, bucket_name: str = None, s3_prefix: str = None):
        self.worker = SingleThreadScraper(headless=headless)
        self.bucket_name = bucket_name # Kept for API compatibility
        self.s3_prefix = s3_prefix     # Kept for API compatibility
    
    def __enter__(self):
        self.worker.__enter__()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.worker.__exit__(exc_type, exc_val, exc_tb)
        
    def scrape_and_upload_matchweek(self, matchweek: int, season: str = "2025/26", delay: float = 2.0, formats: List[str] = None) -> List[dict]:
        """Delegate to worker."""
        formats = formats or ["json"]
        return self.worker.process_matchweek(matchweek, season, delay, formats)


def main():
    parser = argparse.ArgumentParser(description="Premier League Scraper with Parallel Execution")

    parser.add_argument("--match", type=int, help="Single match ID")
    parser.add_argument("--matchweek", nargs='*', type=int, help="Matchweek number(s) to scrape")
    parser.add_argument("--season", type=str, default="2025/26", help="Season")
    parser.add_argument("--formats", type=str, default="json", help="Output formats")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between matches")
    parser.add_argument("--no-headless", action="store_true", help="Show browser")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel threads (default 1)")
    parser.add_argument("--continuous", action="store_true", help="Run indefinitely (scheduled mode)")
    parser.add_argument("--interval", type=int, default=43200, help="Seconds to wait between cycles (default 12h = 43200)")
    parser.add_argument("--run-etl", action="store_true", help="Automatically run ETL after scraping")
    
    args = parser.parse_args()
    formats = [f.strip() for f in args.formats.split(",")]
    
    # CASE 1: Single Match Mode
    if args.match:
        mw = args.matchweek[0] if args.matchweek else None
        with SingleThreadScraper(headless=not args.no_headless) as worker:
            scraper = worker.scraper
            s3 = worker.s3_store
            data = scraper.scrape_match_with_matchweek(args.match, mw, args.season)
            if data:
                s3.upload_match(data, formats)
                print(f"Match {args.match} scraped and uploaded.")
        return

    # CASE 2: Matchweek / Continuous Mode
    if args.matchweek or args.continuous:
        matchweeks = args.matchweek if args.matchweek else list(range(1, 40))
        
        while True:
            logger.info(f"🚀 Starting cycle for season {args.season}...")
            start_time = time.time()
            workers = min(args.workers, len(matchweeks))
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_mw = {
                    executor.submit(
                        run_matchweek_task, 
                        mw, args.season, args.delay, formats, not args.no_headless
                    ): mw for mw in matchweeks
                }
                
                for future in concurrent.futures.as_completed(future_to_mw):
                    mw = future_to_mw[future]
                    try:
                        future.result()
                    except Exception as exc:
                        logger.error(f"MW{mw} generated an exception: {exc}")

            # --- ETL TRIGGER ---
            if args.run_etl:
                logger.info("=== Starting ETL Transformation (Bronze -> Silver -> Gold) ===")
                try:
                    etl = ETLPipeline()
                    # Process current season
                    etl.bronze_to_silver(seasons=[args.season])
                    # Update all teams in Gold
                    etl.silver_to_gold()
                    logger.info("=== ETL Transformation Completed Successfully ===")
                except Exception as e:
                    logger.error(f"ETL failed: {e}")

            duration = time.time() - start_time
            logger.info(f"🏁 Cycle completed in {duration:.2f} seconds.")
            
            if not args.continuous:
                break
            
            logger.info(f"Sleeping for {args.interval}s before next batch check...")
            time.sleep(args.interval)

    else:
        print("Usage: python scrape_to_s3.py --matchweek 1 2 3 --workers 3")

if __name__ == "__main__":
    main()
