"""Kafka Producer for Premier League Scraper."""
import os
import sys
import json
import time
import argparse
from typing import List, Optional
from loguru import logger
from kafka import KafkaProducer

# Add parent directory to path to import local modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import KAFKA_CONFIG, S3_CONFIG
from scraper.season_scraper import SeasonScraper
from data.processor import S3DataStore, S3StatusTracker

def _setup_logging():
    """Configure logging."""
    os.makedirs("logs", exist_ok=True)
    logger.remove()
    logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}", level="INFO")
    logger.add("logs/kafka_producer_{time:YYYY-MM-DD}.log", rotation="1 day", level="DEBUG")

_setup_logging()

class ScraperKafkaProducer:
    """Class to manage scraping and sending data to Kafka."""
    
    def __init__(self, headless: bool = True):
        self.bootstrap_servers = KAFKA_CONFIG.get("bootstrap_servers")
        self.topic = KAFKA_CONFIG.get("topic_raw")
        
        logger.info(f"Connecting to Kafka at {self.bootstrap_servers}...")
        try:
            # Ensure topic exists FIRST before creating producer
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka.errors import TopicAlreadyExistsError
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                request_timeout_ms=5000
            )
            
            if self.topic not in admin_client.list_topics():
                logger.info(f"Creating topic: {self.topic}")
                new_topic = NewTopic(name=self.topic, num_partitions=1, replication_factor=1)
                try:
                    admin_client.create_topics([new_topic])
                    time.sleep(1) # Wait for metadata to propagate
                except TopicAlreadyExistsError:
                    pass
            admin_client.close()

            # Initialize Producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks=1,
                retries=5,
                request_timeout_ms=30000,
                max_block_ms=60000
            )
            
            logger.info(f"Connected to Kafka. Topic: {self.topic}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

        self.scraper = SeasonScraper(headless=headless)
        self.s3_store = S3DataStore()
        self.tracker = S3StatusTracker(flow_name="streaming")

    def __enter__(self):
        self.scraper.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.scraper.stop()
        if self.producer:
            self.producer.close()

    def send_to_kafka(self, data: dict):
        """Send a single match record to Kafka."""
        try:
            future = self.producer.send(self.topic, value=data)
            record_metadata = future.get(timeout=30)
            logger.info(f">>> SENT TO KAFKA: {self.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"Error sending to Kafka: {e}")
            return False

    def process_matchweek(self, matchweek: int, season: str, delay: float) -> int:
        """Scrape all matches for a given matchweek and send to Kafka using 3-field logic."""
        count = 0
        
        logger.info(f"MW{matchweek}: Syncing match statuses for {season} (Streaming Flow)...")
        matches_summary = self.scraper.get_matchweek_matches(matchweek, season)
        
        if not matches_summary:
            logger.warning(f"MW{matchweek}: No matches found. Falling back to ID calculation.")
            match_ids = self.scraper._calculate_match_ids_for_matchweek(matchweek, season)
            matches_summary = [{"match_id": mid, "is_fallback": True} for mid in match_ids]

        for i, match in enumerate(matches_summary):
            match_id = match.get("match_id")
            
            # Field 2: is_played (based on scores from web)
            is_played_on_web = False
            if match.get("is_fallback"):
                is_played_on_web = True
            else:
                hs = match.get("home_score")
                as_ = match.get("away_score")
                if hs is not None and as_ is not None:
                    is_played_on_web = True
            
            # Field 3: is_already_scraped (from S3 manifest, handles 25/26 logic)
            state = self.tracker.get_match_state(match_id, season)
            is_already_scraped = state.get("is_scraped", False)

            # --- DECISION LOGIC ---
            if not is_played_on_web:
                logger.info(f"MW{matchweek} | Match {match_id}: Skipping (Not played yet)")
                continue

            if is_already_scraped:
                logger.info(f"MW{matchweek} | Match {match_id}: Skipping (Already recorded in Streaming manifest)")
                continue
            
            # --- ACTION: SCRAPE & SEND ---
            try:
                logger.info(f"MW{matchweek} | Match {match_id}: Scrape/Send Required -> Processing...")
                match_data = self.scraper.scrape_match_with_matchweek(match_id, matchweek, season)
                
                if match_data:
                    # Double check if played in detail page
                    info = match_data.get("match_info", {})
                    if info.get("home_score") is None or info.get("away_score") is None:
                        logger.info(f"MW{matchweek} | Match {match_id}: Is SCHEDULED. Skipping.")
                        continue

                    match_data["ingestion_timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
                    if self.send_to_kafka(match_data):
                        # Update Tracker on S3
                        self.tracker.update_match_status(match_id, is_played=True, is_scraped=True)
                        count += 1
                
            except Exception as e:
                logger.error(f"MW{matchweek} | Error processing {match_id}: {e}")
            
            if i < len(matches_summary) - 1:
                time.sleep(delay)

        logger.info(f"MW{matchweek}: Completed. {count} new matches sent to Kafka.")
        return count

def main():
    parser = argparse.ArgumentParser(description="Premier League Scraper Kafka Producer")
    parser.add_argument("--matchweek", nargs='*', type=int, help="Specific matchweek(s) to scrape")
    parser.add_argument("--season", type=str, default="2025/26", help="Season (e.g., 2025/26)")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between match scrapes")
    parser.add_argument("--no-headless", action="store_true", help="Run browser in non-headless mode")
    parser.add_argument("--continuous", action="store_true", help="Run indefinitely, checking for updates")
    parser.add_argument("--interval", type=int, default=600, help="Seconds to wait between cycles in continuous mode")
    
    args = parser.parse_args()
    
    if not args.matchweek and not args.continuous:
        logger.error("Please specify at least one matchweek using --matchweek OR use --continuous mode")
        return

    try:
        with ScraperKafkaProducer(headless=not args.no_headless) as producer:
            if args.continuous:
                logger.info(f"🔄 Entering CONTINUOUS mode for season {args.season}")
                logger.info(f"Checking every {args.interval} seconds...")
                
                while True:
                    total_sent = 0
                    # Scan all MWs from 1 to 38 for the current season
                    for mw in range(1, 40): # Premier League usually has 38 MWs
                        try:
                            count = producer.process_matchweek(mw, args.season, args.delay)
                            total_sent += count
                        except Exception as e:
                            logger.error(f"Error in continuous loop for MW{mw}: {e}")
                    
                    logger.info(f"✅ Cycle completed. Total new matches sent to Kafka: {total_sent}")
                    logger.info(f"Sleeping for {args.interval}s before next check...")
                    time.sleep(args.interval)
            else:
                for mw in args.matchweek:
                    producer.process_matchweek(mw, args.season, args.delay)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    except Exception as e:
        logger.critical(f"Producer failed: {e}")

if __name__ == "__main__":
    main()
