"""
Premier League Backfill DAG - Airflow 3.x compatible

DAG để backfill dữ liệu các mùa giải cũ.
Trigger thủ công khi cần cào lại toàn bộ dữ liệu lịch sử.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'bigdata-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=6),
}

dag = DAG(
    'premier_league_backfill',
    default_args=default_args,
    description='Backfill historical Premier League data',
    schedule=None,  # Trigger thủ công
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['premier-league', 'backfill', 'historical'],
    max_active_runs=1,
)

# Danh sách các mùa giải cần backfill
ALL_SEASONS = [
    "2024/25", "2023/24", "2022/23", "2021/22", "2020/21",
    "2019/20", "2018/19", "2017/18", "2016/17", "2015/16",
    "2014/15", "2013/14", "2012/13", "2011/12", "2010/11"
]

def get_seasons_to_process(**context):
    """Lấy danh sách seasons từ DAG config hoặc dùng default"""
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    seasons = conf.get('seasons', ALL_SEASONS)
    workers = conf.get('workers', 3)
    
    print(f"📋 Seasons to process: {seasons}")
    print(f"👷 Workers: {workers}")
    
    context['ti'].xcom_push(key='seasons', value=seasons)
    context['ti'].xcom_push(key='workers', value=workers)
    return seasons

prepare_config = PythonOperator(
    task_id='prepare_config',
    python_callable=get_seasons_to_process,
    dag=dag,
)

# Backfill - chạy cho từng season
backfill_task = BashOperator(
    task_id='backfill_all_seasons',
    bash_command='''
        cd /app
        
        # Danh sách seasons mặc định
        SEASONS="2024/25 2023/24 2022/23 2021/22 2020/21"
        
        echo "🚀 Starting backfill..."
        
        for SEASON in $SEASONS; do
            echo "📅 Processing season: $SEASON"
            python scrape_to_s3.py \
                --matchweek 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 \
                --season "$SEASON" \
                --workers 3 \
                --delay 2.0
            echo "✅ Completed season: $SEASON"
        done
        
        echo "✅ All backfill completed!"
    ''',
    dag=dag,
)

# ETL sau khi backfill
run_full_etl = BashOperator(
    task_id='run_full_etl',
    bash_command='''
        cd /app
        echo "🔄 Running ETL for all seasons..."
        python pipeline.py --all-seasons --create-gold
        echo "✅ ETL completed!"
    ''',
    dag=dag,
)

def log_summary(**context):
    print("=" * 50)
    print("🎉 BACKFILL PIPELINE COMPLETED!")
    print("=" * 50)

summary_task = PythonOperator(
    task_id='log_summary',
    python_callable=log_summary,
    dag=dag,
)

# Dependencies
prepare_config >> backfill_task >> run_full_etl >> summary_task
