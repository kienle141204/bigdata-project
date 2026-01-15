# Airflow Variables Management

Hướng dẫn quản lý Airflow Variables cho dự án Premier League Data Pipeline.

## 📋 Tổng Quan

Airflow Variables cho phép lưu trữ các giá trị cấu hình có thể được sử dụng trong DAGs và tasks. Thay vì hardcode trong code, bạn có thể quản lý tập trung qua Airflow UI hoặc scripts.

## 📁 Files

- `airflow-variables.json` - File JSON chứa tất cả variables (format đơn giản: `{"key": "value", ...}`)
- `import-variables.ps1` - Script PowerShell để import variables vào Airflow
- `export-variables.ps1` - Script PowerShell để export variables từ Airflow
- `import_variables.py` - Python script để import (hỗ trợ cả 2 format JSON)

## 🚀 Cách Sử Dụng

### 1. Chuẩn Bị File Variables

File `airflow-variables.json` sử dụng format đơn giản (Airflow UI format):

```json
{
  "AWS_S3_BUCKET": "your-actual-bucket-name",
  "AWS_ACCESS_KEY_ID": "your-access-key",
  "MYSQL_HOST": "your-mysql-host",
  ...
}
```

**Lưu ý:** Format này tương thích với Airflow UI Import Variables feature.

### 2. Import Variables vào Airflow

**Trên Windows (PowerShell):**
```powershell
cd airflow
.\import-variables.ps1
```

**Trên Linux/Mac:**
```bash
cd airflow
# Sử dụng Python script hoặc Airflow CLI
python import_variables.py
```

### 3. Export Variables từ Airflow

**Trên Windows (PowerShell):**
```powershell
cd airflow
.\export-variables.ps1
```

### 4. Quản Lý Qua Airflow UI

1. Truy cập Airflow UI: http://localhost:8081
2. Vào **Admin** → **Variables**
3. Click **+** để thêm variable mới
4. Hoặc edit/delete variables hiện có

## 📝 Danh Sách Variables

### AWS S3 Configuration
- `AWS_S3_BUCKET` - Tên S3 bucket
- `AWS_ACCESS_KEY_ID` - AWS Access Key
- `AWS_SECRET_ACCESS_KEY` - AWS Secret Key
- `AWS_REGION` - AWS Region (mặc định: ap-southeast-1)
- `AWS_S3_PREFIX` - Prefix trong bucket (mặc định: premier_league)

### MySQL Configuration
- `MYSQL_HOST` - MySQL host
- `MYSQL_PORT` - MySQL port (mặc định: 3306)
- `MYSQL_USER` - MySQL username
- `MYSQL_PASSWORD` - MySQL password
- `MYSQL_DATABASE` - Database name
- `MYSQL_SSL_MODE` - SSL mode (mặc định: REQUIRED)

### Kafka Configuration
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka bootstrap servers
- `KAFKA_TOPIC_RAW` - Kafka topic name (mặc định: premier_league_raw)

### Scraping Configuration
- `CURRENT_SEASON` - Mùa giải hiện tại (mặc định: 2025/26)
- `CURRENT_MATCHWEEK` - Vòng đấu hiện tại
- `SCRAPER_WORKERS` - Số workers song song (mặc định: 3)
- `SCRAPER_DELAY` - Delay giữa các request (mặc định: 2.0 giây)
- `HEADLESS_MODE` - Chạy headless (mặc định: true)

### Spark Configuration
- `SPARK_DRIVER_MEMORY` - Driver memory (mặc định: 4g)
- `SPARK_EXECUTOR_MEMORY` - Executor memory (mặc định: 2g)

## 💡 Sử Dụng Variables Trong DAGs

### Cách 1: Sử dụng Variable.get()

```python
from airflow.models import Variable

# Lấy variable
aws_bucket = Variable.get("AWS_S3_BUCKET")
mysql_host = Variable.get("MYSQL_HOST")

# Lấy với giá trị mặc định
workers = Variable.get("SCRAPER_WORKERS", default_var=3)
```

### Cách 2: Sử dụng Jinja Template

```python
from airflow.operators.bash import BashOperator

task = BashOperator(
    task_id='scrape_task',
    bash_command='python scrape_to_s3.py --bucket {{ var.value.AWS_S3_BUCKET }}',
    dag=dag,
)
```

### Cách 3: Sử dụng Variable trong Environment Variables

```python
from airflow.operators.bash import BashOperator

task = BashOperator(
    task_id='scrape_task',
    bash_command='python scrape_to_s3.py',
    env={
        'AWS_S3_BUCKET': '{{ var.value.AWS_S3_BUCKET }}',
        'AWS_ACCESS_KEY_ID': '{{ var.value.AWS_ACCESS_KEY_ID }}',
    },
    dag=dag,
)
```

## 🔐 Security Best Practices

1. **Không commit file có credentials thực tế**
   - File `airflow-variables.json` chỉ chứa giá trị mẫu
   - Sử dụng `.gitignore` để ignore file có credentials

2. **Sử dụng Airflow Connections cho sensitive data**
   - AWS credentials → Airflow Connections
   - MySQL credentials → Airflow Connections
   - Variables chỉ dùng cho non-sensitive config

3. **Rotate credentials định kỳ**
   - Thay đổi passwords mỗi 90 ngày
   - Update variables sau khi rotate

## 🐛 Troubleshooting

### Variables không được load

```bash
# Kiểm tra variables trong container
docker exec airflow airflow variables list

# Hoặc qua Python
docker exec airflow python -c "from airflow.models import Variable; print(Variable.get('AWS_S3_BUCKET'))"
```

### Lỗi khi import

```bash
# Kiểm tra format JSON
python -m json.tool airflow-variables.json

# Kiểm tra container logs
docker logs airflow | grep -i variable
```

## 📚 Tài Liệu Tham Khảo

- [Airflow Variables Documentation](https://airflow.apache.org/docs/apache-airflow/stable/concepts/variables.html)
- [Airflow Connections](https://airflow.apache.org/docs/apache-airflow/stable/concepts/connections.html)
