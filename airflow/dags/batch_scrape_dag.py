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
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# ================== CẤU HÌNH ==================
CURRENT_MATCHWEEK = 20
SEASON = "2025/26"
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

# ============ TASK 1: Scrape Current Season ============
scrape_current_season = BashOperator(
    task_id='scrape_current_season',
    bash_command='''
        cd /app && python scrape_to_s3.py \
            --matchweek 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 \
            --season "2025/26" \
            --workers 3 \
            --delay 2.0
        
        echo ""
        echo "Smart skip logic applied (see data/processor.py for CURRENT_MATCHWEEK)"
    ''',
    dag=dag,
)

# ============ TASK 2: Run ETL Bronze to Silver ============
etl_bronze_to_silver = BashOperator(
    task_id='etl_bronze_to_silver',
    bash_command='cd /app && python pipeline.py --all-seasons',
    dag=dag,
)

# ============ TASK 3: Run ETL Silver to Gold ============
etl_silver_to_gold = BashOperator(
    task_id='etl_silver_to_gold',
    bash_command='cd /app && python pipeline.py --gold-only',
    dag=dag,
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
scrape_current_season >> etl_bronze_to_silver >> etl_silver_to_gold >> cleanup_task >> notify_success
