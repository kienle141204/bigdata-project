#!/bin/bash
# Deploy Airflow on EC2 without Git
# Usage: curl -sSL <url> | bash
# Or: bash deploy_airflow_ec2.sh

set -e

echo "🚀 Deploying Airflow for Premier League Batch Pipeline..."

# Create directory structure
mkdir -p ~/bigdata-airflow/dags ~/bigdata-airflow/logs ~/bigdata-airflow/plugins
cd ~/bigdata-airflow

# Create .env file (EDIT THIS!)
cat > .env << 'ENVFILE'
# ⚠️ EDIT THESE VALUES!
AWS_S3_BUCKET=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=ap-southeast-1
AWS_S3_PREFIX=premier_league
HEADLESS=true
REQUEST_DELAY=2
AIRFLOW_UID=1000
ENVFILE

echo "📝 Created .env file - PLEASE EDIT with your AWS credentials!"

# Create docker-compose.airflow.yml
cat > docker-compose.airflow.yml << 'COMPOSE'
version: '3.8'

x-airflow-common:
  &airflow-common
  image: kienle1412/bigdata:latest
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__DEFAULT_TIMEZONE: 'Asia/Ho_Chi_Minh'
    AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE: 'Asia/Ho_Chi_Minh'
    AWS_S3_BUCKET: ${AWS_S3_BUCKET}
    AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
    AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    AWS_REGION: ${AWS_REGION:-ap-southeast-1}
    AWS_S3_PREFIX: ${AWS_S3_PREFIX:-premier_league}
    HEADLESS: 'true'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:14
    container_name: airflow-postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: unless-stopped

  airflow-webserver:
    <<: *airflow-common
    container_name: airflow-webserver
    command: bash -c "pip install apache-airflow[postgres] && airflow webserver"
    ports:
      - "8081:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: unless-stopped
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: bash -c "pip install apache-airflow[postgres] && airflow scheduler"
    shm_size: "2gb"
    restart: unless-stopped
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-init:
    <<: *airflow-common
    container_name: airflow-init
    entrypoint: /bin/bash
    command:
      - -c
      - |
        pip install apache-airflow[postgres]
        airflow db init
        airflow users create \
          --username admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@localhost \
          --password admin
        echo "Airflow initialized!"
    user: "0:0"

volumes:
  postgres-db-volume:
COMPOSE

echo "📝 Created docker-compose.airflow.yml"

# Create batch_scrape_dag.py
cat > dags/batch_scrape_dag.py << 'DAG1'
"""Premier League Batch Scraping DAG - Daily at 6:00 AM"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

CURRENT_MATCHWEEK = 20
SEASON = "2025/26"

default_args = {
    'owner': 'bigdata-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'premier_league_batch_scrape',
    default_args=default_args,
    description=f'Daily scraping {SEASON} - Current MW: {CURRENT_MATCHWEEK}',
    schedule_interval='0 6 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['premier-league', 'batch'],
    max_active_runs=1,
)

scrape_task = BashOperator(
    task_id='scrape_current_season',
    bash_command='''
        cd /app && python scrape_to_s3.py \
            --matchweek 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 \
            --season "2025/26" \
            --workers 3 \
            --delay 2.0
    ''',
    dag=dag,
)

etl_bronze_silver = BashOperator(
    task_id='etl_bronze_to_silver',
    bash_command='cd /app && python pipeline.py --all-seasons',
    dag=dag,
)

etl_silver_gold = BashOperator(
    task_id='etl_silver_to_gold',
    bash_command='cd /app && python pipeline.py --gold-only',
    dag=dag,
)

def notify(**context):
    print(f"✅ Pipeline completed! Season: {SEASON}, MW: {CURRENT_MATCHWEEK}")

notify_task = PythonOperator(
    task_id='notify_success',
    python_callable=notify,
    dag=dag,
)

scrape_task >> etl_bronze_silver >> etl_silver_gold >> notify_task
DAG1

echo "📝 Created dags/batch_scrape_dag.py"

# Set permissions
chmod -R 777 dags logs plugins

echo ""
echo "✅ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "   1. Edit .env file with your AWS credentials:"
echo "      nano .env"
echo ""
echo "   2. Start Airflow:"
echo "      sudo docker-compose -f docker-compose.airflow.yml up -d"
echo ""
echo "   3. Wait ~60 seconds for initialization, then access:"
echo "      http://$(curl -s ifconfig.me 2>/dev/null || echo '<EC2-IP>'):8081"
echo "      Username: admin"
echo "      Password: admin"
