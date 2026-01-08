"""S3 Data Store for uploading scraped match data."""
import json
import io
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
from loguru import logger


class S3DataStore:
    """Upload scraped data directly to S3."""
    
    def __init__(self, bucket_name: str = None, aws_access_key_id: str = None, 
                 aws_secret_access_key: str = None, region_name: str = None, prefix: str = "premier_league"):
        import boto3
        from config.settings import S3_CONFIG
        
        self.bucket_name = bucket_name or S3_CONFIG.get("bucket_name")
        self.prefix = prefix or S3_CONFIG.get("prefix", "premier_league")
        self.region_name = region_name or S3_CONFIG.get("region_name", "ap-southeast-1")
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id or S3_CONFIG.get("aws_access_key_id"),
            aws_secret_access_key=aws_secret_access_key or S3_CONFIG.get("aws_secret_access_key"),
            region_name=self.region_name
        )
        logger.info(f"Connected to S3 bucket: {self.bucket_name}")
    
    def _generate_s3_key(self, season: str = None, matchweek: int = None, match_id: int = None, ext: str = "json") -> str:
        """Generate S3 key path."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if season and matchweek and match_id:
            return f"{self.prefix}/{season.replace('/', '-')}/matchweek_{matchweek:02d}/match_{match_id}.{ext}"
        elif season and matchweek:
            return f"{self.prefix}/{season.replace('/', '-')}/aggregates/mw{matchweek:02d}_{timestamp}.{ext}"
        else:
            return f"{self.prefix}/data/{timestamp}.{ext}"
    
    def upload_json(self, data: Dict[str, Any], s3_key: str = None) -> Optional[str]:
        """Upload data as JSON to S3."""
        from botocore.exceptions import ClientError
        
        if not s3_key:
            s3_key = self._generate_s3_key(data.get("season"), data.get("matchweek"), data.get("match_id"))
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8'),
                ContentType='application/json'
            )
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Uploaded: {s3_uri}")
            return s3_uri
        except ClientError as e:
            logger.error(f"Upload failed: {e}")
            return None
    
    def upload_csv(self, data: Dict[str, Any], s3_key: str = None) -> Optional[str]:
        """Upload match stats as CSV to S3."""
        from botocore.exceptions import ClientError
        
        if not s3_key:
            s3_key = self._generate_s3_key(data.get("season"), data.get("matchweek"), data.get("match_id"), "csv")
        
        try:
            rows = []
            match_info = data.get("match_info", {})
            for stat_name, stat_values in data.get("statistics", {}).items():
                if isinstance(stat_values, dict):
                    rows.append({
                        "match_id": data.get("match_id"),
                        "home_team": match_info.get("home_team"),
                        "away_team": match_info.get("away_team"),
                        "stat_name": stat_name,
                        "home_value": stat_values.get("home"),
                        "away_value": stat_values.get("away"),
                    })
            
            csv_buffer = io.StringIO()
            pd.DataFrame(rows).to_csv(csv_buffer, index=False)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue().encode('utf-8'),
                ContentType='text/csv'
            )
            return f"s3://{self.bucket_name}/{s3_key}"
        except ClientError as e:
            logger.error(f"CSV upload failed: {e}")
            return None
    
    def upload_match(self, data: Dict[str, Any], formats: List[str] = None) -> Dict[str, Optional[str]]:
        """Upload match in multiple formats."""
        formats = formats or ["json"]
        results = {}
        if "json" in formats:
            results["json"] = self.upload_json(data)
        if "csv" in formats:
            results["csv"] = self.upload_csv(data)
        return results
    
    def upload_aggregate(self, matches: List[Dict[str, Any]], season: str = None, matchweek: int = None) -> Optional[str]:
        """Upload aggregated match data."""
        from botocore.exceptions import ClientError
        
        s3_key = self._generate_s3_key(season, matchweek)
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(matches, indent=2, ensure_ascii=False).encode('utf-8'),
                ContentType='application/json'
            )
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Aggregate uploaded: {s3_uri}")
            return s3_uri
        except ClientError as e:
            logger.error(f"Aggregate upload failed: {e}")
            return None
