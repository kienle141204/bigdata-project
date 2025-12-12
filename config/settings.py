import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent

# ==================== AWS S3 Data Lake ====================
AWS_CONFIG = {
    "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region": os.getenv("AWS_REGION", "ap-southeast-1"),
    "s3_bucket": os.getenv("AWS_S3_BUCKET", "your-datalake-bucket"),
}

# S3 Data Lake Paths (trên AWS)
S3_PATHS = {
    "raw": "raw/",           # Bronze layer
    "processed": "processed/",  # Silver layer
    "curated": "curated/",      # Gold layer
    "archive": "archive/",
}

# Kafka settings
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "consumer_group": os.getenv("KAFKA_CONSUMER_GROUP", "bigdata-group"),
}

# Database settings (example)
DATABASE_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "bigdata_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Logging settings
LOG_CONFIG = {
    "log_dir": BASE_DIR / "logs",
    "log_level": os.getenv("LOG_LEVEL", "INFO"),
    "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
}
