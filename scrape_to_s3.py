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
from data.processor import S3DataStore
from data.db import MatchDB


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
        self.db = MatchDB()
        self.s3_store = S3DataStore()
        
    def __enter__(self):
        self.scraper.start()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.scraper.stop()

    def process_matchweek(self, matchweek: int, season: str, delay: float, formats: List[str]) -> List[dict]:
        """Scrape all matches for a given matchweek."""
        results = []
        all_matches = []
        match_ids = self.scraper._calculate_match_ids_for_matchweek(matchweek, season)
        
        logger.info(f"MW{matchweek}: Starting processing ({len(match_ids)} matches)")
        
        for i, match_id in enumerate(match_ids):
            # CHECK DB
            current_status = self.db.get_match_status(match_id)
            if current_status == "PLAYED":
                logger.info(f"MW{matchweek} | Match {match_id}: Skipping (Already PLAYED)")
                continue 
            
            # Scrape
            try:
                match_data = self.scraper.scrape_match_with_matchweek(match_id, matchweek, season)
                if match_data:
                    all_matches.append(match_data)
                    upload_results = self.s3_store.upload_match(match_data, formats)
                    
                    # UPDATE DB logic
                    info = match_data.get("match_info", {})
                    status = "SCHEDULED"
                    hs = info.get("home_score")
                    as_ = info.get("away_score")
                    try:
                        hs = int(hs) if hs is not None else None
                        as_ = int(as_) if as_ is not None else None
                    except: pass
                        
                    if isinstance(hs, int) and isinstance(as_, int):
                        status = "PLAYED"
                    
                    self.db.update_match_status(match_id, status)
                    
                    results.append({
                        "match_id": match_id,
                        "home": info.get("home_team"),
                        "away": info.get("away_team")
                    })
            except Exception as e:
                logger.error(f"MW{matchweek} | Error scraping {match_id}: {e}")
            
            if i < len(match_ids) - 1:
                time.sleep(delay)

        # Upload Aggregate for this MW
        if all_matches:
            self.s3_store.upload_aggregate(all_matches, season, matchweek)
            logger.info(f"MW{matchweek}: Uploaded aggregate data.")
            
        logger.info(f"MW{matchweek}: Completed. {len(results)} matches scraped.")
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
    parser.add_argument("--bucket", type=str, help="S3 bucket name")
    parser.add_argument("--prefix", type=str, help="S3 key prefix")
    parser.add_argument("--formats", type=str, default="json", help="Output formats")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between matches")
    parser.add_argument("--no-headless", action="store_true", help="Show browser")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel threads (default 1)")
    
    args = parser.parse_args()
    formats = [f.strip() for f in args.formats.split(",")]
    
    # CASE 1: Single Match Mode (No Parallel needed implies workers=1 essentially)
    if args.match:
        mw = args.matchweek[0] if args.matchweek else None
        # Reuse the SingleThreadScraper class for consistency
        with SingleThreadScraper(headless=not args.no_headless) as worker:
            # We need to manually access the internal logic or just expose a method
            # For simplicity, let's just do the manual scrape here reusing the component
            scraper = worker.scraper
            s3 = worker.s3_store
            
            data = scraper.scrape_match_with_matchweek(args.match, mw, args.season)
            if data:
                s3.upload_match(data, formats)
                print(f"Match {args.match} scraped and uploaded.")
        return

    # CASE 2: Matchweek Mode (Parallel supported)
    if args.matchweek:
        matchweeks = args.matchweek
        workers = min(args.workers, len(matchweeks)) # Don't spawn more threads than tasks
        
        logger.info(f"🚀 Starting scrape for {len(matchweeks)} matchweeks using {workers} parallel threads.")
        
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            # Map each matchweek to the worker function
            future_to_mw = {
                executor.submit(
                    run_matchweek_task, 
                    mw, args.season, args.delay, formats, not args.no_headless
                ): mw for mw in matchweeks
            }
            
            for future in concurrent.futures.as_completed(future_to_mw):
                mw = future_to_mw[future]
                try:
                    res = future.result()
                    # logger.info(f"Thread for MW{mw} finished.")
                except Exception as exc:
                    logger.error(f"MW{mw} generated an exception: {exc}")

        duration = time.time() - start_time
        logger.info(f"🏁 All tasks completed in {duration:.2f} seconds.")

    else:
        print("Usage: python scrape_to_s3.py --matchweek 1 2 3 --workers 3")

if __name__ == "__main__":
    main()
