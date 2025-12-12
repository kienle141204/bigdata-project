"""
Data Lake Paths Configuration
S3 path definitions for Data Lake on AWS
"""
from dataclasses import dataclass
from datetime import datetime
import os


@dataclass
class DataLakePaths:
    """S3 paths for different data lake layers"""
    
    bucket: str = os.getenv("AWS_S3_BUCKET", "your-datalake-bucket")
    
    # Bronze Layer - Raw data
    raw: str = "raw/"
    
    # Silver Layer - Processed data
    processed: str = "processed/"
    
    # Gold Layer - Curated/Analytics ready
    curated: str = "curated/"
    
    # Archive
    archive: str = "archive/"
    
    def get_raw_path(self, source: str = "topcv.vn", dt: datetime = None) -> str:
        """
        Get S3 path for raw data (Bronze layer)
        Format: raw/jobs/source={source}/year={Y}/month={M}/day={D}/
        """
        dt = dt or datetime.now()
        return (
            f"s3://{self.bucket}/{self.raw}jobs/"
            f"source={source}/"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        )
    
    def get_processed_path(self, source: str = "topcv.vn", dt: datetime = None) -> str:
        """
        Get S3 path for processed data (Silver layer)
        Format: processed/jobs/source={source}/year={Y}/month={M}/day={D}/
        """
        dt = dt or datetime.now()
        return (
            f"s3://{self.bucket}/{self.processed}jobs/"
            f"source={source}/"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        )
    
    def get_curated_path(self, use_case: str) -> str:
        """
        Get S3 path for curated/analytics data (Gold layer)
        Format: curated/{use_case}/
        """
        return f"s3://{self.bucket}/{self.curated}{use_case}/"
    
    def get_archive_path(self, year: int = None) -> str:
        """Get S3 path for archived data"""
        year = year or datetime.now().year
        return f"s3://{self.bucket}/{self.archive}{year}/"
    
    # Prefixes for listing objects
    @property
    def raw_jobs_prefix(self) -> str:
        return f"{self.raw}jobs/"
    
    @property
    def processed_jobs_prefix(self) -> str:
        return f"{self.processed}jobs/"
