"""
AWS S3 Data Lake Client
Module for uploading/downloading data from S3 Data Lake
"""
import os
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from .paths import DataLakePaths


class S3DataLakeClient:
    """Client for working with AWS S3 Data Lake"""
    
    def __init__(
        self,
        bucket_name: Optional[str] = None,
        aws_access_key: Optional[str] = None,
        aws_secret_key: Optional[str] = None,
        region: Optional[str] = None
    ):
        """Initialize S3 client"""
        self.bucket = bucket_name or os.getenv("AWS_S3_BUCKET")
        self.region = region or os.getenv("AWS_REGION", "ap-southeast-1")
        
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key or os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=aws_secret_key or os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=self.region
        )
        
        self.paths = DataLakePaths(bucket=self.bucket)
    
    # ==================== UPLOAD METHODS ====================
    
    def upload_file(self, local_path: str, s3_key: str) -> bool:
        """Upload a single file to S3"""
        try:
            self.s3_client.upload_file(local_path, self.bucket, s3_key)
            print(f"✅ Uploaded: {local_path} -> s3://{self.bucket}/{s3_key}")
            return True
        except ClientError as e:
            print(f"❌ Upload failed: {e}")
            return False
    
    def upload_json(self, data: Dict[str, Any], s3_key: str) -> bool:
        """Upload JSON data directly to S3"""
        try:
            json_data = json.dumps(data, ensure_ascii=False, indent=2)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=json_data.encode("utf-8"),
                ContentType="application/json"
            )
            print(f"✅ Uploaded JSON: s3://{self.bucket}/{s3_key}")
            return True
        except ClientError as e:
            print(f"❌ Upload JSON failed: {e}")
            return False
    
    def upload_to_raw(self, local_path: str, source: str, filename: Optional[str] = None) -> bool:
        """Upload file to raw layer (Bronze) with date partition"""
        today = datetime.now()
        date_partition = today.strftime("%Y/%m/%d")
        fname = filename or Path(local_path).name
        s3_key = f"raw/{source}/{date_partition}/{fname}"
        return self.upload_file(local_path, s3_key)
    
    def upload_to_processed(self, local_path: str, domain: str, filename: Optional[str] = None) -> bool:
        """Upload file to processed layer (Silver)"""
        fname = filename or Path(local_path).name
        s3_key = f"processed/{domain}/{fname}"
        return self.upload_file(local_path, s3_key)
    
    def upload_to_curated(self, local_path: str, use_case: str, filename: Optional[str] = None) -> bool:
        """Upload file to curated layer (Gold)"""
        fname = filename or Path(local_path).name
        s3_key = f"curated/{use_case}/{fname}"
        return self.upload_file(local_path, s3_key)
    
    # ==================== DOWNLOAD METHODS ====================
    
    def download_file(self, s3_key: str, local_path: str) -> bool:
        """Download a file from S3"""
        try:
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            self.s3_client.download_file(self.bucket, s3_key, local_path)
            print(f"✅ Downloaded: s3://{self.bucket}/{s3_key} -> {local_path}")
            return True
        except ClientError as e:
            print(f"❌ Download failed: {e}")
            return False
    
    def read_json(self, s3_key: str) -> Optional[Dict[str, Any]]:
        """Read JSON file directly from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except ClientError as e:
            print(f"❌ Read JSON failed: {e}")
            return None
    
    # ==================== LIST METHODS ====================
    
    def list_objects(self, prefix: str, max_keys: int = 1000) -> List[str]:
        """List objects in S3 with given prefix"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            return [obj["Key"] for obj in response.get("Contents", [])]
        except ClientError as e:
            print(f"❌ List objects failed: {e}")
            return []
    
    def list_raw_files(self, source: str) -> List[str]:
        """List all files in raw layer for a source"""
        return self.list_objects(f"raw/{source}/")
    
    def list_processed_files(self, domain: str) -> List[str]:
        """List all files in processed layer for a domain"""
        return self.list_objects(f"processed/{domain}/")
    
    # ==================== DELETE METHODS ====================
    
    def delete_object(self, s3_key: str) -> bool:
        """Delete an object from S3"""
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=s3_key)
            print(f"🗑️ Deleted: s3://{self.bucket}/{s3_key}")
            return True
        except ClientError as e:
            print(f"❌ Delete failed: {e}")
            return False
    
    def move_to_archive(self, s3_key: str) -> bool:
        """Move an object to archive layer"""
        try:
            year = datetime.now().strftime("%Y")
            archive_key = f"archive/{year}/{s3_key}"
            self.s3_client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": s3_key},
                Key=archive_key
            )
            self.delete_object(s3_key)
            print(f"📦 Archived: {s3_key} -> {archive_key}")
            return True
        except ClientError as e:
            print(f"❌ Archive failed: {e}")
            return False
