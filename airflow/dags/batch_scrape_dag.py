"""
Premier League Batch Scraping DAG - Airflow 3.x compatible

Chạy hàng ngày để:
1. Cào dữ liệu từ Premier League website
2. Upload lên S3 Bronze layer
3. Xử lý ETL: Bronze -> Silver -> Gold

Logic thông minh:
- Vòng < CURRENT_MATCHWEEK: Nếu đã scraped --> bỏ qua
- Vòng >= CURRENT_MATCHWEEK: Kiểm tra score mỗi lần
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# ================== CẤU HÌNH ==================
# Đọc từ Airflow Variables (fallback về giá trị mặc định)
CURRENT_MATCHWEEK = int(Variable.get("CURRENT_MATCHWEEK", default_var="20"))
SEASON = Variable.get("CURRENT_SEASON", default_var="2025/26")
SCRAPER_WORKERS = Variable.get("SCRAPER_WORKERS", default_var="3")
SCRAPER_DELAY = Variable.get("SCRAPER_DELAY", default_var="2.0")
# ==============================================

default_args = {
    'owner': 'bigdata-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'premier_league_batch_scrape',
    default_args=default_args,
    description=f'Daily Premier League scraping ({SEASON}) - Current MW: {CURRENT_MATCHWEEK}',
    schedule='0 6 * * *',  # Chạy lúc 6:00 AM mỗi ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['premier-league', 'scraping', 'batch', 'etl'],
    max_active_runs=1,
)

# ============ TASK 1: Set Environment Variables ============
def set_env_vars_from_airflow(**context):
    """Set environment variables từ Airflow Variables để các scripts có thể đọc"""
    import os
    from airflow.models import Variable
    
    var_keys = [
        'AWS_S3_BUCKET', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION', 'AWS_S3_PREFIX',
        'MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE', 'MYSQL_SSL_MODE', 'MYSQL_SSL_CA',
        'KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC_RAW',
        'HEADLESS', 'HEADLESS_MODE', 'REQUEST_DELAY', 'SCRAPER_DELAY'
    ]
    
    for key in var_keys:
        try:
            value = Variable.get(key, default_var=None)
            if value:
                os.environ[key] = value
        except:
            pass
    
    print("✅ Environment variables set from Airflow Variables")

set_env_task = PythonOperator(
    task_id='set_environment_variables',
    python_callable=set_env_vars_from_airflow,
    dag=dag,
)

# ============ TASK 2: Scrape Current Season ============
scrape_current_season = BashOperator(
    task_id='scrape_current_season',
    bash_command=f'''
        cd /app && python scrape_to_s3.py \
            --matchweek 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 \
            --season "{SEASON}" \
            --workers {SCRAPER_WORKERS} \
            --delay {SCRAPER_DELAY}
        
        echo ""
        echo "Smart skip logic applied (see data/processor.py for CURRENT_MATCHWEEK)"
    ''',
    dag=dag,
    env={
        'AWS_S3_BUCKET': '{{ var.value.AWS_S3_BUCKET }}',
        'AWS_ACCESS_KEY_ID': '{{ var.value.AWS_ACCESS_KEY_ID }}',
        'AWS_SECRET_ACCESS_KEY': '{{ var.value.AWS_SECRET_ACCESS_KEY }}',
        'AWS_REGION': '{{ var.value.AWS_REGION }}',
        'AWS_S3_PREFIX': '{{ var.value.AWS_S3_PREFIX }}',
        'HEADLESS': '{{ var.value.HEADLESS }}',
        'HEADLESS_MODE': '{{ var.value.HEADLESS_MODE }}',
        'REQUEST_DELAY': '{{ var.value.REQUEST_DELAY }}',
        'SCRAPER_DELAY': '{{ var.value.SCRAPER_DELAY }}',
        'PYTHONUNBUFFERED': '1',
    },
)

# ============ TASK 3: Run ETL Bronze to Silver ============
etl_bronze_to_silver = BashOperator(
    task_id='etl_bronze_to_silver',
    bash_command='cd /app && python pipeline.py --all-seasons',
    dag=dag,
    env={
        'AWS_S3_BUCKET': '{{ var.value.AWS_S3_BUCKET }}',
        'AWS_ACCESS_KEY_ID': '{{ var.value.AWS_ACCESS_KEY_ID }}',
        'AWS_SECRET_ACCESS_KEY': '{{ var.value.AWS_SECRET_ACCESS_KEY }}',
        'AWS_REGION': '{{ var.value.AWS_REGION }}',
        'AWS_S3_PREFIX': '{{ var.value.AWS_S3_PREFIX }}',
    },
)

# ============ TASK 4: Run ETL Silver to Gold ============
etl_silver_to_gold = BashOperator(
    task_id='etl_silver_to_gold',
    bash_command='cd /app && python pipeline.py --gold-only',
    dag=dag,
    env={
        'AWS_S3_BUCKET': '{{ var.value.AWS_S3_BUCKET }}',
        'AWS_ACCESS_KEY_ID': '{{ var.value.AWS_ACCESS_KEY_ID }}',
        'AWS_SECRET_ACCESS_KEY': '{{ var.value.AWS_SECRET_ACCESS_KEY }}',
        'AWS_REGION': '{{ var.value.AWS_REGION }}',
        'AWS_S3_PREFIX': '{{ var.value.AWS_S3_PREFIX }}',
    },
)

# ============ TASK 4: Cleanup ============
cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='''
        echo "Cleaning up temporary files..."
        rm -rf /tmp/chrome* /tmp/*.tmp 2>/dev/null || true
        echo "Cleanup completed."
    ''',
    dag=dag,
)

# ============ TASK 5: Notification ============
def send_success_notification(**context):
    """Log success notification"""
    print(f"✅ Premier League batch pipeline completed successfully!")
    print(f"📊 Season: {SEASON} | Current Matchweek: {CURRENT_MATCHWEEK}")
    print(f"🔄 Data has been processed to Gold layer")

notify_success = PythonOperator(
    task_id='notify_success',
    python_callable=send_success_notification,
    dag=dag,
)

# ============ TASK DEPENDENCIES ============
set_env_task >> scrape_current_season >> etl_bronze_to_silver >> etl_silver_to_gold >> cleanup_task >> notify_success
