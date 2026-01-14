"""Kafka configuration settings."""
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Settings
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "client_id": os.getenv("KAFKA_CLIENT_ID", "premier_league_scraper"),
    
    # Producer settings
    "producer": {
        "acks": "all",  # Wait for all replicas to acknowledge
        "retries": 3,
        "max_in_flight_requests_per_connection": 1,
        "compression_type": "gzip",
        "value_serializer": lambda v: v.encode('utf-8') if isinstance(v, str) else v,
    },
    
    # Consumer settings
    "consumer": {
        "group_id": os.getenv("KAFKA_CONSUMER_GROUP", "premier_league_etl_group"),
        "auto_offset_reset": "earliest",  # Read from beginning if no offset
        "enable_auto_commit": False,  # Manual commit for reliability
        "max_poll_records": 10,
        "session_timeout_ms": 30000,
    }
}

# Kafka Topics
TOPICS = {
    "RAW_MATCH_DATA": "raw-match-data",
    "SILVER_PROCESSING": "silver-processing",
    "DEAD_LETTER_QUEUE": "dlq-failed-messages"
}

# Topic configurations
TOPIC_CONFIGS = {
    TOPICS["RAW_MATCH_DATA"]: {
        "num_partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": "604800000",  # 7 days
            "compression.type": "gzip"
        }
    },
    TOPICS["SILVER_PROCESSING"]: {
        "num_partitions": 2,
        "replication_factor": 1,
        "config": {
            "retention.ms": "259200000",  # 3 days
        }
    },
    TOPICS["DEAD_LETTER_QUEUE"]: {
        "num_partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": "2592000000",  # 30 days
        }
    }
}
