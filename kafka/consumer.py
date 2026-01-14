"""Kafka Consumer for processing match data and uploading to Silver."""
import json
import signal
import sys
from typing import Dict, Any, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from loguru import logger

from kafka.config import KAFKA_CONFIG, TOPICS
from data.processor import S3DataStore
from data.etl import ETLPipeline


class MatchDataConsumer:
    """Consumer to process match data from Kafka and upload to S3."""
    
    def __init__(self, bootstrap_servers: str = None, group_id: str = None):
        """
        Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka broker address
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers or KAFKA_CONFIG["bootstrap_servers"]
        self.group_id = group_id or KAFKA_CONFIG["consumer"]["group_id"]
        self.consumer = None
        self.s3_store = S3DataStore()
        self.etl = ETLPipeline()
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.warning(f"Received signal {signum}. Shutting down gracefully...")
        self.running = False
    
    def _connect(self):
        """Connect to Kafka broker."""
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False,  # Manual commit for reliability
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                max_poll_records=10,
                session_timeout_ms=30000
            )
            logger.info(f"✅ Kafka Consumer connected to {self.bootstrap_servers}")
            logger.info(f"   Group ID: {self.group_id}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise
    
    def subscribe(self, topics: list = None):
        """
        Subscribe to Kafka topics.
        
        Args:
            topics: List of topics to subscribe (default: raw-match-data)
        """
        if not self.consumer:
            self._connect()
        
        topics = topics or [TOPICS["RAW_MATCH_DATA"]]
        self.consumer.subscribe(topics)
        logger.info(f"📡 Subscribed to topics: {', '.join(topics)}")
    
    def _process_match_data(self, match_data: Dict[str, Any]) -> bool:
        """
        Process a single match data message.
        
        Flow:
        1. Upload raw data to Bronze layer (S3)
        2. Log success
        
        Args:
            match_data: Match data dictionary
        
        Returns:
            True if successful
        """
        try:
            match_id = match_data.get("match_id")
            season = match_data.get("season")
            matchweek = match_data.get("matchweek")
            
            logger.info(f"Processing: Match {match_id} | {season} | MW{matchweek}")
            
            # Upload to Bronze (S3)
            result = self.s3_store.upload_json(match_data, layer="bronze")
            
            if result:
                logger.success(f"✅ Bronze uploaded: Match {match_id}")
                return True
            else:
                logger.error(f"❌ Failed to upload Match {match_id} to Bronze")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error processing match data: {e}")
            return False
    
    def _process_silver_trigger(self, message: Dict[str, Any]) -> bool:
        """
        Process Silver layer ETL trigger.
        
        Args:
            message: Trigger message with season/matchweek info
        
        Returns:
            True if successful
        """
        try:
            action = message.get("action")
            season = message.get("season")
            matchweek = message.get("matchweek")
            
            if action != "process_silver":
                logger.warning(f"Unknown action: {action}")
                return False
            
            logger.info(f"🔄 Starting Silver ETL: {season} MW{matchweek or 'ALL'}")
            
            # Run ETL pipeline
            if matchweek:
                # Process specific matchweek (not implemented yet, run full season)
                self.etl.bronze_to_silver(seasons=[season])
            else:
                # Process entire season
                self.etl.bronze_to_silver(seasons=[season])
            
            logger.success(f"✅ Silver ETL completed: {season}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing Silver trigger: {e}")
            return False
    
    def start(self):
        """Start consuming messages."""
        if not self.consumer:
            self._connect()
        
        logger.info("🚀 Starting Kafka Consumer...")
        logger.info("   Press Ctrl+C to stop")
        
        processed_count = 0
        failed_count = 0
        
        try:
            while self.running:
                # Poll messages
                msg_pack = self.consumer.poll(timeout_ms=1000)
                
                if not msg_pack:
                    continue
                
                for topic_partition, messages in msg_pack.items():
                    for message in messages:
                        try:
                            # Determine message type by topic
                            if message.topic == TOPICS["RAW_MATCH_DATA"]:
                                success = self._process_match_data(message.value)
                            elif message.topic == TOPICS["SILVER_PROCESSING"]:
                                success = self._process_silver_trigger(message.value)
                            else:
                                logger.warning(f"Unknown topic: {message.topic}")
                                success = False
                            
                            if success:
                                processed_count += 1
                                # Commit offset after successful processing
                                self.consumer.commit()
                            else:
                                failed_count += 1
                                logger.warning(f"Failed to process message from {message.topic}")
                                # Optionally send to DLQ
                                self._send_to_dlq(message)
                        
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            failed_count += 1
                            self._send_to_dlq(message)
                
                # Log progress periodically
                if (processed_count + failed_count) % 10 == 0 and processed_count > 0:
                    logger.info(f"📊 Progress: {processed_count} processed, {failed_count} failed")
        
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        
        finally:
            logger.info(f"📊 Final Stats: {processed_count} processed, {failed_count} failed")
            self.close()
    
    def _send_to_dlq(self, message):
        """Send failed message to Dead Letter Queue."""
        try:
            from kafka import KafkaProducer
            
            dlq_producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            dlq_data = {
                "original_topic": message.topic,
                "original_partition": message.partition,
                "original_offset": message.offset,
                "value": message.value,
                "timestamp": str(message.timestamp)
            }
            
            dlq_producer.send(TOPICS["DEAD_LETTER_QUEUE"], value=dlq_data)
            dlq_producer.flush()
            dlq_producer.close()
            
            logger.info(f"📮 Sent to DLQ: {message.topic}[{message.partition}]@{message.offset}")
        
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
    
    def close(self):
        """Close consumer connection."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka Consumer closed")


def run_consumer(topics: list = None):
    """
    Run Kafka consumer.
    
    Args:
        topics: List of topics to consume (default: raw-match-data)
    """
    consumer = MatchDataConsumer()
    consumer.subscribe(topics)
    consumer.start()


if __name__ == "__main__":
    # Run consumer for raw match data by default
    run_consumer()
