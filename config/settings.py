"""Configuration settings for Premier League scraper."""
import os
from dotenv import load_dotenv

load_dotenv()

# Chỉ đọc từ environment variables (đã được set từ Airflow Variables qua BashOperator)
# Không đọc trực tiếp từ Airflow Variables vì script này có thể chạy standalone
def get_config_value(key: str, default: str = None) -> str:
    """
    Lấy giá trị từ environment variables.
    Khi chạy trong Airflow, các variables đã được set vào env từ BashOperator.
    Khi chạy standalone, đọc từ .env file.
    """
    return os.getenv(key, default)

# Selenium
SELENIUM_CONFIG = {
    "headless": get_config_value("HEADLESS", "true").lower() == "true",
    "implicit_wait": 10,
    "page_load_timeout": 30,
    "window_size": (1920, 1080),
}

# Scraping
SCRAPING_CONFIG = {
    "base_url": "https://www.premierleague.com",
    "match_url_template": "https://www.premierleague.com/match/{match_id}",
    "request_delay": float(get_config_value("REQUEST_DELAY", "2")),
}

# CSS Selectors
CSS_SELECTORS = {
    "stats_tab": "[data-tab-index='3'], a[href*='stats']",
    "cookie_accept": "#onetrust-accept-btn-handler",
}

# Stats categories
STATS_CATEGORIES = ["Top Stats", "Attack", "Possession", "Defence", "Physical", "Discipline"]

# AWS S3
S3_CONFIG = {
    "bucket_name": get_config_value("AWS_S3_BUCKET", ""),
    "aws_access_key_id": get_config_value("AWS_ACCESS_KEY_ID", ""),
    "aws_secret_access_key": get_config_value("AWS_SECRET_ACCESS_KEY", ""),
    "region_name": get_config_value("AWS_REGION", "ap-southeast-1"),
    "prefix": get_config_value("AWS_S3_PREFIX", "premier_league"),
}

# Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": get_config_value("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "topic_raw": get_config_value("KAFKA_TOPIC_RAW", "premier_league_raw"),
}

# MySQL Database
MYSQL_CONFIG = {
    "host": get_config_value("MYSQL_HOST", "mysql-6d9700d-quydang16012004-e907.f.aivencloud.com"),
    "port": int(get_config_value("MYSQL_PORT", "28225")),
    "user": get_config_value("MYSQL_USER", "avnadmin"),
    "password": get_config_value("MYSQL_PASSWORD", ""),
    "database": get_config_value("MYSQL_DATABASE", "defaultdb"),
    "ssl_mode": get_config_value("MYSQL_SSL_MODE", "REQUIRED"),
    "ssl_ca": get_config_value("MYSQL_SSL_CA", ""),
}
