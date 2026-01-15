"""
Utility functions để đọc từ Airflow Variables hoặc environment variables
Sử dụng khi chạy trong Airflow context hoặc standalone
"""
import os

def get_config_value(key: str, default: str = None) -> str:
    """
    Lấy giá trị từ Airflow Variables hoặc environment variables
    
    Args:
        key: Tên của variable
        default: Giá trị mặc định nếu không tìm thấy
        
    Returns:
        Giá trị của variable hoặc default
    """
    # Thử đọc từ Airflow Variables trước (nếu đang chạy trong Airflow)
    try:
        from airflow.models import Variable
        value = Variable.get(key, default_var=None)
        if value:
            return value
    except (ImportError, Exception):
        # Không phải trong Airflow context hoặc Variable không tồn tại
        pass
    
    # Fallback về environment variable
    return os.getenv(key, default)

def set_env_from_variables():
    """
    Set tất cả environment variables từ Airflow Variables
    Hữu ích khi cần set env vars cho các scripts Python chạy từ BashOperator
    """
    try:
        from airflow.models import Variable
        
        # Danh sách các variables cần set
        var_keys = [
            'AWS_S3_BUCKET', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION', 'AWS_S3_PREFIX',
            'MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE', 'MYSQL_SSL_MODE', 'MYSQL_SSL_CA',
            'KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC_RAW',
            'CURRENT_SEASON', 'CURRENT_MATCHWEEK',
            'SCRAPER_WORKERS', 'SCRAPER_DELAY', 'REQUEST_DELAY',
            'HEADLESS', 'HEADLESS_MODE',
            'SPARK_DRIVER_MEMORY', 'SPARK_EXECUTOR_MEMORY'
        ]
        
        for key in var_keys:
            try:
                value = Variable.get(key, default_var=None)
                if value:
                    os.environ[key] = value
            except:
                # Variable không tồn tại, giữ nguyên env var nếu có
                pass
    except (ImportError, Exception):
        # Không phải trong Airflow context
        pass
