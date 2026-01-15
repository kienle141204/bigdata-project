"""
Premier League Spark MySQL Transformation DAG - Airflow 3.x compatible

DAG để transform dữ liệu từ Gold layer (S3) sang MySQL database.
Chạy sau khi Gold layer đã được tạo từ batch_scrape_dag.

Luồng xử lý:
1. Đọc dữ liệu từ S3 Gold layer (matches.csv, players.csv, events.csv)
2. Transform và clean dữ liệu với Spark
3. Load vào MySQL database (tran_dau, doi_bong, cau_thu, cau_thu_tran_dau, su_kien_tran_dau)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# ================== CẤU HÌNH ==================
# Season để transform (None = tất cả seasons)
DEFAULT_SEASON = None  # Ví dụ: "2024/25" hoặc None cho tất cả
# ==============================================

default_args = {
    'owner': 'bigdata-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),  # Spark transformation có thể mất thời gian
}

dag = DAG(
    'spark_mysql_transformation',
    default_args=default_args,
    description='Transform Premier League Gold data (S3) to MySQL database using Spark',
    schedule='0 7 * * *',  # Chạy lúc 7:00 AM mỗi ngày (sau batch_scrape_dag 1 giờ)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['premier-league', 'spark', 'mysql', 'transformation', 'gold'],
    max_active_runs=1,
)

# ============ TASK 1: Validate Environment ============
def validate_environment(**context):
    """Kiểm tra Airflow Variables có tồn tại"""
    required_vars = [
        'AWS_S3_BUCKET',
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'MYSQL_HOST',
        'MYSQL_PASSWORD',
        'MYSQL_DATABASE'
    ]
    
    missing_vars = []
    var_values = {}
    
    # Đọc từ Airflow Variables (chỉ để validate)
    # BashOperator sẽ tự động set env vars từ Jinja template {{ var.value.XXX }}
    for var in required_vars:
        try:
            value = Variable.get(var, default_var=None)
            if value:
                var_values[var] = value
            else:
                missing_vars.append(var)
        except Exception:
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required Airflow Variables: {', '.join(missing_vars)}. Please import variables first!")
    
    print("✅ All required Airflow Variables are set")
    print(f"📦 S3 Bucket: {var_values.get('AWS_S3_BUCKET')}")
    print(f"🗄️ MySQL Host: {var_values.get('MYSQL_HOST')}")
    print(f"🗄️ MySQL Database: {var_values.get('MYSQL_DATABASE')}")
    print("ℹ️  Variables will be passed to scripts via BashOperator env parameter")

validate_env = PythonOperator(
    task_id='validate_environment',
    python_callable=validate_environment,
    dag=dag,
)

# ============ TASK 2: Transform All Gold Data to MySQL ============
def get_season_to_process(**context):
    """Lấy season từ DAG config hoặc dùng default"""
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    season = conf.get('season', DEFAULT_SEASON)
    
    if season:
        print(f"📅 Processing season: {season}")
    else:
        print("📅 Processing all seasons")
    
    context['ti'].xcom_push(key='season', value=season)
    return season

get_season = PythonOperator(
    task_id='get_season_to_process',
    python_callable=get_season_to_process,
    dag=dag,
)

# Transform dữ liệu từ Gold layer sang MySQL
transform_to_mysql = BashOperator(
    task_id='transform_gold_to_mysql',
    bash_command='''
        cd /app
        
        # Tìm Python executable - thử nhiều đường dẫn
        PYTHON_CMD=""
        for py_path in "/usr/local/bin/python3" "/usr/local/bin/python" "/usr/bin/python3" "/usr/bin/python" "$(which python3 2>/dev/null)" "$(which python 2>/dev/null)"; do
            if [ -n "$py_path" ] && [ -f "$py_path" ] && [ -x "$py_path" ]; then
                PYTHON_CMD="$py_path"
                break
            fi
        done
        
        if [ -z "$PYTHON_CMD" ]; then
            echo "ERROR: Python executable not found!"
            exit 1
        fi
        
        # Set PYSPARK_PYTHON với đường dẫn đầy đủ (quan trọng cho Spark)
        export PYSPARK_PYTHON="$PYTHON_CMD"
        export PYSPARK_DRIVER_PYTHON="$PYTHON_CMD"
        
        echo "🐍 Found Python: $PYSPARK_PYTHON"
        $PYSPARK_PYTHON --version || echo "Warning: Cannot verify Python version"
        
        # Lấy season từ XCom (nếu có)
        SEASON="{{ ti.xcom_pull(task_ids='get_season_to_process', key='season') }}"
        
        echo "🚀 Starting Spark transformation: Gold (S3) -> MySQL"
        echo "📅 Season: ${SEASON:-all seasons}"
        
        # Chạy Spark transformation
        if [ -z "$SEASON" ] || [ "$SEASON" = "None" ]; then
            echo "🔄 Processing all seasons..."
            $PYTHON_CMD Spark/s3_to_mysql_transformer.py
        else
            # Convert season format từ "2024/25" sang "2024-25" (Gold layer dùng dấu gạch ngang)
            SEASON_FORMATTED=$(echo "$SEASON" | sed 's|/|-|g')
            echo "🔄 Processing season: $SEASON (formatted: $SEASON_FORMATTED)"
            $PYTHON_CMD Spark/s3_to_mysql_transformer.py --season "$SEASON_FORMATTED"
        fi
        
        echo "✅ Spark transformation completed!"
    ''',
    dag=dag,
    env={
        'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64',
        'SPARK_LOCAL_IP': '127.0.0.1',
        'SPARK_DRIVER_MEMORY': '{{ var.value.SPARK_DRIVER_MEMORY }}',
        'SPARK_EXECUTOR_MEMORY': '{{ var.value.SPARK_EXECUTOR_MEMORY }}',
        'PYTHONUNBUFFERED': '1',
        # Python executable cho Spark - tìm đường dẫn đầy đủ
        # Airflow image có Python ở /usr/local/bin/python hoặc /usr/bin/python3
        # Python executable sẽ được tìm tự động trong bash script
        # Không set ở đây để bash script có thể tìm đúng đường dẫn
        # AWS S3
        'AWS_S3_BUCKET': '{{ var.value.AWS_S3_BUCKET }}',
        'AWS_ACCESS_KEY_ID': '{{ var.value.AWS_ACCESS_KEY_ID }}',
        'AWS_SECRET_ACCESS_KEY': '{{ var.value.AWS_SECRET_ACCESS_KEY }}',
        'AWS_REGION': '{{ var.value.AWS_REGION }}',
        'AWS_S3_PREFIX': '{{ var.value.AWS_S3_PREFIX }}',
        # MySQL
        'MYSQL_HOST': '{{ var.value.MYSQL_HOST }}',
        'MYSQL_PORT': '{{ var.value.MYSQL_PORT }}',
        'MYSQL_USER': '{{ var.value.MYSQL_USER }}',
        'MYSQL_PASSWORD': '{{ var.value.MYSQL_PASSWORD }}',
        'MYSQL_DATABASE': '{{ var.value.MYSQL_DATABASE }}',
        'MYSQL_SSL_MODE': '{{ var.value.MYSQL_SSL_MODE }}',
    },
)

# ============ TASK 3: Verify MySQL Data ============
def verify_mysql_data(**context):
    """Kiểm tra dữ liệu đã được load vào MySQL"""
    import os
    import sys
    
    # Add project root to path
    sys.path.insert(0, '/app')
    
    try:
        from Spark.connectors.spark_mysql_connector import create_spark_session_with_mysql
        
        spark, jdbc_url, conn_props = create_spark_session_with_mysql(
            app_name="MySQLVerification",
            include_s3=False
        )
        
        tables_to_check = [
            'tran_dau',
            'doi_bong', 
            'cau_thu',
            'cau_thu_tran_dau',
            'su_kien_tran_dau'
        ]
        
        print("=" * 60)
        print("📊 VERIFYING MYSQL DATA")
        print("=" * 60)
        
        for table in tables_to_check:
            try:
                df = spark.read.jdbc(
                    url=jdbc_url,
                    table=table,
                    properties=conn_props
                )
                count = df.count()
                print(f"✅ {table}: {count:,} records")
            except Exception as e:
                print(f"⚠️ {table}: Table not found or error - {e}")
        
        spark.stop()
        print("=" * 60)
        print("✅ Verification completed!")
        
    except Exception as e:
        print(f"⚠️ Verification failed: {e}")
        # Không fail DAG nếu verification thất bại
        pass

verify_data = PythonOperator(
    task_id='verify_mysql_data',
    python_callable=verify_mysql_data,
    dag=dag,
)

# ============ TASK 4: Notification ============
def send_success_notification(**context):
    """Log success notification"""
    season = context['ti'].xcom_pull(task_ids='get_season_to_process', key='season')
    
    print("=" * 60)
    print("✅ SPARK MYSQL TRANSFORMATION COMPLETED!")
    print("=" * 60)
    print(f"📅 Season: {season if season else 'All seasons'}")
    print(f"🔄 Data has been transformed from Gold (S3) to MySQL")
    print("=" * 60)

notify_success = PythonOperator(
    task_id='notify_success',
    python_callable=send_success_notification,
    dag=dag,
)

# ============ TASK DEPENDENCIES ============
validate_env >> get_season >> transform_to_mysql >> verify_data >> notify_success
