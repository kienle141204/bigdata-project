# Scrapy settings for crawler project

BOT_NAME = "crawler"

SPIDER_MODULES = ["crawler.web_crawler"]
NEWSPIDER_MODULE = "crawler.web_crawler"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Robots.txt (disabled for TopCV)
ROBOTSTXT_OBEY = False

# Request settings
CONCURRENT_REQUESTS = 4
DOWNLOAD_DELAY = 1
COOKIES_ENABLED = True

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

# AWS S3 Configuration
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET', 'your-bucket-name')
AWS_REGION = os.getenv('AWS_REGION', 'ap-southeast-1')

# Item Pipelines
ITEM_PIPELINES = {
    "crawler.pipelines.S3Pipeline": 300,
}

# Logging
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'

# AutoThrottle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

# Retry
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]
