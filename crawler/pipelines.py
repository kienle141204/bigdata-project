"""
S3 Pipelines for Job Crawler
Save crawled data to Amazon S3 with date-partitioned structure
"""

import json
import hashlib
import boto3
from datetime import datetime
from botocore.exceptions import ClientError


class S3Pipeline:
    """
    Pipeline to save data to S3
    
    Structure: s3://bucket/raw/jobs/source=topcv.vn/year=2024/month=12/day=12/job_xxx.json
    """
    
    def __init__(self, aws_access_key, aws_secret_key, bucket_name, region_name='ap-southeast-1'):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )
        self.items_saved = 0
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            aws_access_key=crawler.settings.get('AWS_ACCESS_KEY'),
            aws_secret_key=crawler.settings.get('AWS_SECRET_KEY'),
            bucket_name=crawler.settings.get('S3_BUCKET_NAME'),
            region_name=crawler.settings.get('AWS_REGION', 'ap-southeast-1')
        )
    
    def open_spider(self, spider):
        spider.logger.info(f"S3Pipeline opened - Bucket: {self.bucket_name}")
        # Cache existing job_ids to avoid duplicates
        self.existing_jobs = self._load_existing_job_ids()
        spider.logger.info(f"Loaded {len(self.existing_jobs)} existing job IDs from S3")
    
    def close_spider(self, spider):
        spider.logger.info(f"S3Pipeline closed - Saved {self.items_saved} items")
    
    def _load_existing_job_ids(self):
        """Load list of existing job IDs from S3"""
        existing = set()
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix='raw/jobs/'):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith('.json'):
                        job_id = key.split('/')[-1].replace('.json', '')
                        existing.add(job_id)
        except Exception:
            pass  # If error, skip duplicate check
        return existing
    
    def process_item(self, item, spider):
        try:
            s3_key = self._generate_s3_key(item)
            job_id = s3_key.split('/')[-1].replace('.json', '')
            
            # Skip if already exists
            if job_id in self.existing_jobs:
                spider.logger.debug(f"Skipped duplicate: {job_id}")
                return item
            
            json_data = json.dumps(dict(item), ensure_ascii=False, indent=2)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            
            self.items_saved += 1
            self.existing_jobs.add(job_id)
            spider.logger.info(f"Saved to S3: {s3_key}")
            
        except ClientError as e:
            spider.logger.error(f"Failed to save to S3: {e}")
            raise
        
        return item
    
    def _generate_s3_key(self, item):
        """Generate S3 key with partitioned structure"""
        crawled_at = item.get('crawled_at', datetime.now().isoformat())
        dt = datetime.fromisoformat(crawled_at) if isinstance(crawled_at, str) else crawled_at
        
        url = item.get('url', '')
        job_id = hashlib.md5(url.encode()).hexdigest()[:12] if url else f"unknown_{dt.strftime('%Y%m%d%H%M%S%f')}"
        source = item.get('source', 'unknown')
        
        return f"raw/jobs/source={source}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/job_{job_id}.json"


class RawHtmlPipeline(S3Pipeline):
    """Pipeline for RawHtmlSpider - saves to raw_html folder"""
    
    def _generate_s3_key(self, item):
        crawled_at = item.get('crawled_at', datetime.now().isoformat())
        dt = datetime.fromisoformat(crawled_at) if isinstance(crawled_at, str) else crawled_at
        
        url = item.get('url', '')
        job_id = hashlib.md5(url.encode()).hexdigest()[:12] if url else f"unknown_{dt.strftime('%Y%m%d%H%M%S%f')}"
        source = item.get('source', 'unknown')
        
        return f"raw_html/source={source}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/job_{job_id}.json"


class JobStatusPipeline(S3Pipeline):
    """Pipeline for JobStatusSpider - saves to job_status folder"""
    
    def _generate_s3_key(self, item):
        checked_at = item.get('checked_at', datetime.now().isoformat())
        dt = datetime.fromisoformat(checked_at) if isinstance(checked_at, str) else checked_at
        
        url = item.get('url', '')
        job_id = hashlib.md5(url.encode()).hexdigest()[:12] if url else f"unknown_{dt.strftime('%Y%m%d%H%M%S%f')}"
        
        return f"job_status/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/job_{job_id}.json"
