# Data Lake Module
# Code để làm việc với AWS S3 Data Lake

from .s3_client import S3DataLakeClient
from .paths import DataLakePaths

__all__ = ["S3DataLakeClient", "DataLakePaths"]
