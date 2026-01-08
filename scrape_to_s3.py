"""Premier League Scraper with S3 upload."""
import os
import sys
import json
import time
import argparse
from typing import List, Optional
from datetime import datetime
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import S3_CONFIG
from scraper.season_scraper import SeasonScraper
from data.processor import S3DataStore


class S3ScraperApp:
    """Scraper application that uploads directly to S3."""
    
    def __init__(self, headless: bool = True, bucket_name: str = None, s3_prefix: str = None):
        self.headless = headless
        self.scraper = None
        self.s3_store = None
        self.bucket_name = bucket_name
        self.s3_prefix = s3_prefix
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure logging."""
        os.makedirs("logs", exist_ok=True)
        logger.remove()
        logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}", level="INFO")
        logger.add("logs/scraper_{time:YYYY-MM-DD}.log", rotation="1 day", level="DEBUG")
    
    def start(self):
        """Initialize scraper and S3 store."""
        logger.info("Starting S3 Scraper")
        self.scraper = SeasonScraper(headless=self.headless)
        self.scraper.start()
        self.s3_store = S3DataStore(bucket_name=self.bucket_name, prefix=self.s3_prefix)
        logger.info("Application started")
    
    def stop(self):
        """Shutdown scraper."""
        logger.info("Shutting down...")
        if self.scraper:
            self.scraper.stop()
        logger.info("Shutdown complete")
    
    def scrape_and_upload_match(self, match_id: int, matchweek: int = None, season: str = "2025/26", formats: List[str] = None) -> Optional[dict]:
        """Scrape a single match and upload to S3."""
        formats = formats or ["json"]
        logger.info(f"Scraping match {match_id}")
        
        try:
            match_data = self.scraper.scrape_match_with_matchweek(match_id, matchweek, season)
            if match_data:
                upload_results = self.s3_store.upload_match(match_data, formats)
                logger.info(f"Match {match_id} uploaded to S3")
                return {
                    "match_id": match_id,
                    "matchweek": matchweek,
                    "season": season,
                    "s3_uris": upload_results,
                    "match_info": match_data.get("match_info", {}),
                }
            return None
        except Exception as e:
            logger.error(f"Error scraping match {match_id}: {e}")
            return None
    
    def scrape_and_upload_matchweek(self, matchweek: int, season: str = "2025/26", delay: float = 2.0, formats: List[str] = None) -> List[dict]:
        """Scrape all matches from a matchweek and upload to S3."""
        formats = formats or ["json"]
        results = []
        all_matches = []
        
        match_ids = self.scraper._calculate_match_ids_for_matchweek(matchweek, season)
        total = len(match_ids)
        logger.info(f"MW{matchweek}: {total} matches ({match_ids[0]} to {match_ids[-1]})")
        
        for i, match_id in enumerate(match_ids):
            logger.info(f"  [{i+1}/{total}] Match {match_id}")
            
            match_data = self.scraper.scrape_match_with_matchweek(match_id, matchweek, season)
            if match_data:
                all_matches.append(match_data)
                upload_results = self.s3_store.upload_match(match_data, formats)
                results.append({
                    "match_id": match_id,
                    "s3_uris": upload_results,
                    "home_team": match_data.get("match_info", {}).get("home_team"),
                    "away_team": match_data.get("match_info", {}).get("away_team"),
                })
            
            if i < total - 1:
                time.sleep(delay)
        
        # Upload aggregate
        if all_matches:
            self.s3_store.upload_aggregate(all_matches, season, matchweek)
            
            # Also upload a simple CSV summary for Bronze
            summary_rows = []
            for m in all_matches:
                info = m.get("match_info", {})
                summary_rows.append({
                    "match_id": m.get("match_id"),
                    "season": m.get("season"),
                    "matchweek": m.get("matchweek"),
                    "date": info.get("date_time") or info.get("date"),
                    "home_team": info.get("home_team"),
                    "away_team": info.get("away_team"),
                    "home_score": info.get("home_score"),
                    "away_score": info.get("away_score"),
                    "url": m.get("url")
                })
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_key = f"{self.s3_prefix}/bronze/{season.replace('/', '-')}/aggregates/mw{matchweek:02d}_summary_{timestamp}.csv"
            self.s3_store.upload_csv(summary_rows, layer="bronze", s3_key=csv_key)
        
        logger.info(f"MW{matchweek} complete: {len(results)}/{total} matches")
        return results
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


def main():
    parser = argparse.ArgumentParser(description="Premier League Scraper with S3 Upload")
    parser.add_argument("--match", type=int, help="Single match ID")
    parser.add_argument("--matchweek", type=int, help="Matchweek number (1-38)")
    parser.add_argument("--season", type=str, default="2025/26", help="Season (e.g., 2024/25)")
    parser.add_argument("--bucket", type=str, help="S3 bucket name")
    parser.add_argument("--prefix", type=str, help="S3 key prefix")
    parser.add_argument("--formats", type=str, default="json", help="Output formats (json,csv)")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between scrapes")
    parser.add_argument("--no-headless", action="store_true", help="Show browser")
    
    args = parser.parse_args()
    formats = [f.strip() for f in args.formats.split(",")]
    
    with S3ScraperApp(headless=not args.no_headless, bucket_name=args.bucket, s3_prefix=args.prefix) as app:
        if args.match:
            result = app.scrape_and_upload_match(args.match, args.matchweek, args.season, formats)
            if result:
                print(json.dumps(result, indent=2))
        
        elif args.matchweek:
            results = app.scrape_and_upload_matchweek(args.matchweek, args.season, args.delay, formats)
            print(f"\n{'='*50}")
            print(f"MATCHWEEK {args.matchweek} - {len(results)} matches")
            print('='*50)
            for r in results:
                print(f"  • {r['match_id']}: {r.get('home_team')} vs {r.get('away_team')}")
        
        else:
            print("Usage: python scrape_to_s3.py --matchweek 1 --season 2024/25")


if __name__ == "__main__":
    main()
