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
    
    def _generate_s3_key(self, layer: str = "bronze", season: str = None, matchweek: int = None, match_id: int = None, ext: str = "json") -> str:
        """Generate S3 key path with layer support."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Ensure layer is valid
        if layer not in ["bronze", "silver", "gold"]:
            layer = "bronze"
            
        base_path = f"{self.prefix}/{layer}"
        
        if season:
            season_fmt = season.replace('/', '-')
            if matchweek:
                if match_id:
                    return f"{base_path}/{season_fmt}/matchweek_{matchweek:02d}/match_{match_id}.{ext}"
                return f"{base_path}/{season_fmt}/aggregates/mw{matchweek:02d}_{timestamp}.{ext}"
            return f"{base_path}/{season_fmt}/data/{timestamp}.{ext}"
        
        return f"{base_path}/misc/{timestamp}.{ext}"
    
    def upload_json(self, data: Dict[str, Any], layer: str = "bronze", s3_key: str = None) -> Optional[str]:
        """Upload data as JSON to S3."""
        from botocore.exceptions import ClientError
        
        if not s3_key:
            s3_key = self._generate_s3_key(
                layer=layer,
                season=data.get("season"), 
                matchweek=data.get("matchweek"), 
                match_id=data.get("match_id")
            )
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8'),
                ContentType='application/json'
            )
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Uploaded to {layer}: {s3_uri}")
            return s3_uri
        except ClientError as e:
            logger.error(f"Upload failed: {e}")
            return None
    
    def upload_csv(self, data: Dict[str, Any], layer: str = "bronze", s3_key: str = None) -> Optional[str]:
        """Upload match stats as CSV to S3."""
        from botocore.exceptions import ClientError
        
        if not s3_key:
            s3_key = self._generate_s3_key(
                layer=layer,
                season=data.get("season"), 
                matchweek=data.get("matchweek"), 
                match_id=data.get("match_id"), 
                ext="csv"
            )
        
        try:
            # Check if input is already a list of dicts (flat structure)
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                # Convert nested match object to flat rows
                rows = []
                match_info = data.get("match_info", {})
                match_id = data.get("match_id")
                
                # If we're uploading to gold, we might get a different structure, 
                # but for bronze/silver match dumping, this logic holds.
                # If 'statistics' is missing, maybe it's already flattened?
                
                statistics = data.get("statistics")
                if statistics and isinstance(statistics, dict):
                    for stat_name, stat_values in statistics.items():
                        if isinstance(stat_values, dict):
                            rows.append({
                                "match_id": match_id,
                                "home_team": match_info.get("home_team"),
                                "away_team": match_info.get("away_team"),
                                "stat_name": stat_name,
                                "home_value": stat_values.get("home"),
                                "away_value": stat_values.get("away"),
                            })
                elif not statistics and isinstance(data, dict):
                    # Fallback for simple dict upload
                    rows = [data]

                df = pd.DataFrame(rows)
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
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
    
    def upload_match(self, data: Dict[str, Any], formats: List[str] = None, layer: str = "bronze") -> Dict[str, Optional[str]]:
        """Upload match in multiple formats."""
        formats = formats or ["json"]
        results = {}
        if "json" in formats:
            results["json"] = self.upload_json(data, layer=layer)
        if "csv" in formats:
            results["csv"] = self.upload_csv(data, layer=layer)
        return results
    
    def upload_aggregate(self, matches: List[Dict[str, Any]], season: str = None, matchweek: int = None, layer: str = "bronze") -> Optional[str]:
        """Upload aggregated match data."""
        from botocore.exceptions import ClientError
        
        s3_key = self._generate_s3_key(layer=layer, season=season, matchweek=matchweek)
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(matches, indent=2, ensure_ascii=False).encode('utf-8'),
                ContentType='application/json'
            )
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Aggregate uploaded to {layer}: {s3_uri}")
            return s3_uri
        except ClientError as e:
            logger.error(f"Aggregate upload failed: {e}")
            return None

    def list_files(self, layer: str, season: str, matchweek: int = None, ext: str = "json") -> List[str]:
        """List files in a specific layer path matching an extension."""
        prefix_path = f"{self.prefix}/{layer}/{season.replace('/', '-')}/"
        if matchweek:
            prefix_path += f"matchweek_{matchweek:02d}/"
            
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix_path)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(f".{ext}")]
            return []
        except Exception as e:
            logger.error(f"List files failed: {e}")
            return []

    def read_json(self, key: str) -> Optional[Dict]:
        """Read a JSON file from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except Exception as e:
            logger.error(f"Read failed for {key}: {e}")
            return None

    def read_csv(self, key: str) -> Optional[pd.DataFrame]:
        """Read a CSV file from S3 into a DataFrame."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return pd.read_csv(response['Body'])
        except Exception as e:
            logger.error(f"Read CSV failed for {key}: {e}")
            return None

    def list_aggregates(self, layer: str, season: str, ext: str = "csv") -> List[str]:
        """List aggregate files for a season."""
        prefix_path = f"{self.prefix}/{layer}/{season.replace('/', '-')}/aggregates/"
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix_path)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(f".{ext}")]
            return []
        except Exception as e:
            logger.error(f"List aggregates failed: {e}")
            return []
