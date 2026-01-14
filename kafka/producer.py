"""Kafka Producer for sending scraped match data."""
import json
import pandas as pd
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

from kafka.config import KAFKA_CONFIG, TOPICS


class MatchDataProducer:
    """Producer to send match data to Kafka."""
    
    def __init__(self, bootstrap_servers: str = None):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker address (default from config)
        """
        self.bootstrap_servers = bootstrap_servers or KAFKA_CONFIG["bootstrap_servers"]
        self.producer = None
        self._connect()
    
    def _connect(self):
        """Connect to Kafka broker."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks='all',
                retries=3,
                compression_type='gzip',
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"✅ Kafka Producer connected to {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise
    
    def send_match_data(self, match_data: Dict[str, Any], topic: str = None) -> bool:
        """
        Send match data to Kafka topic.
        
        Args:
            match_data: Dictionary containing match information
            topic: Target topic (default: raw-match-data)
        
        Returns:
            True if successful, False otherwise
        """
        topic = topic or TOPICS["RAW_MATCH_DATA"]
        
        try:
            # Extract key for partitioning (use match_id)
            match_id = match_data.get("match_id")
            season = match_data.get("season", "unknown")
            matchweek = match_data.get("matchweek", 0)
            
            # Create message key for partitioning
            key = f"{season}_{matchweek}_{match_id}".encode('utf-8') if match_id else None
            
            # Send message
            future = self.producer.send(
                topic,
                key=key,
                value=match_data
            )
            
            # Wait for acknowledgment (blocking)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"📨 Sent to Kafka | Topic: {topic} | "
                f"Partition: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset} | "
                f"Match: {match_id}"
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Kafka error sending match {match_data.get('match_id')}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            return False
    
    def send_batch(self, matches: list, topic: str = None) -> dict:
        """
        Send multiple matches to Kafka.
        
        Args:
            matches: List of match dictionaries
            topic: Target topic
        
        Returns:
            Dictionary with success/failure counts
        """
        topic = topic or TOPICS["RAW_MATCH_DATA"]
        results = {"success": 0, "failed": 0}
        
        for match in matches:
            if self.send_match_data(match, topic):
                results["success"] += 1
            else:
                results["failed"] += 1
        
        # Flush remaining messages
        self.producer.flush()
        
        logger.info(f"📊 Batch complete: {results['success']} success, {results['failed']} failed")
        return results
    
    def trigger_silver_processing(self, season: str, matchweek: int = None):
        """
        Trigger Silver layer ETL processing.
        
        Args:
            season: Season to process
            matchweek: Optional specific matchweek
        """
        message = {
            "action": "process_silver",
            "season": season,
            "matchweek": matchweek,
            "timestamp": str(pd.Timestamp.now())
        }
        
        try:
            future = self.producer.send(
                TOPICS["SILVER_PROCESSING"],
                value=message
            )
            future.get(timeout=10)
            logger.info(f"🔔 Triggered Silver processing for {season} MW{matchweek or 'ALL'}")
        except Exception as e:
            logger.error(f"Failed to trigger Silver processing: {e}")
    
    def close(self):
        """Close producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka Producer closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Convenience function
def send_to_kafka(match_data: Dict[str, Any]) -> bool:
    """
    Quick function to send a single match to Kafka.
    
    Args:
        match_data: Match dictionary
    
    Returns:
        True if successful
    """
    with MatchDataProducer() as producer:
        return producer.send_match_data(match_data)
