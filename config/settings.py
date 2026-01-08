"""Configuration settings for Premier League scraper."""
import os
from dotenv import load_dotenv

load_dotenv()

# Selenium
SELENIUM_CONFIG = {
    "headless": os.getenv("HEADLESS", "true").lower() == "true",
    "implicit_wait": 10,
    "page_load_timeout": 30,
    "window_size": (1920, 1080),
}

# Scraping
SCRAPING_CONFIG = {
    "base_url": "https://www.premierleague.com",
    "match_url_template": "https://www.premierleague.com/match/{match_id}",
    "request_delay": float(os.getenv("REQUEST_DELAY", "2")),
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
    "bucket_name": os.getenv("AWS_S3_BUCKET", ""),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    "region_name": os.getenv("AWS_REGION", "ap-southeast-1"),
    "prefix": os.getenv("AWS_S3_PREFIX", "premier_league"),
}
