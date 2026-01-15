import os
import sys
import json
from datetime import datetime
from loguru import logger

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.processor import S3DataStore
from scraper.season_scraper import SeasonScraper

def init_s3_status():
    store = S3DataStore()
    scraper = SeasonScraper()
    
    # Seasons to mark as "Scraped" in Batch
    batch_seasons = [
        "2010/11", "2011/12", "2012/13", "2013/14", "2014/15",
        "2015/16", "2016/17", "2017/18", "2018/19", "2019/20",
        "2020/21", "2021/22", "2022/23", "2023/24", "2024/25"
    ]
    
    batch_status = {}
    timestamp = datetime.now().isoformat()
    
    logger.info("Generating status for Batch flow...")
    for season in batch_seasons:
        season_config = scraper.SEASONS.get(season)
        if not season_config:
            continue
            
        start_id = season_config["start_match_id"]
        # Total 380 matches per season
        for i in range(380):
            match_id = start_id + i
            batch_status[str(match_id)] = {
                "is_played": True,
                "is_scraped": True,
                "last_updated": timestamp,
                "note": f"Auto-initialized for season {season}"
            }
    
    # Upload Batch Status
    batch_key = f"{store.prefix}/metadata/batch_status.json"
    store.upload_json(batch_status, s3_key=batch_key)
    logger.success(f"Uploaded batch_status.json with {len(batch_status)} matches to {batch_key}")
    
    # Upload Streaming Status (Empty = all False by default)
    streaming_status = {}
    streaming_key = f"{store.prefix}/metadata/streaming_status.json"
    store.upload_json(streaming_status, s3_key=streaming_key)
    logger.success(f"Uploaded streaming_status.json (empty) to {streaming_key}")

if __name__ == "__main__":
    init_s3_status()
