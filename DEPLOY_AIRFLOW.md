# 🌊 AIRFLOW BATCH PIPELINE - Hướng dẫn triển khai

Pipeline batch xử lý dữ liệu Premier League với Apache Airflow.

## 📋 Mô tả

Luồng batch này bao gồm:
1. **Airflow Scheduler**: Tự động trigger job theo lịch (mỗi ngày 1 lần)
2. **Batch Scraper**: Cào dữ liệu từ Premier League website
3. **ETL Pipeline**: Bronze → Silver → Gold transformation
4. **Airflow UI**: Giao diện theo dõi và quản lý các job

## 📁 Cấu trúc thư mục Airflow

```
airflow/
├── dags/
│   ├── batch_scrape_dag.py      # DAG scraping hàng ngày
│   └── backfill_dag.py          # DAG backfill dữ liệu cũ
├── docker-compose.airflow.yml   # Docker Compose cho Airflow
├── Dockerfile.airflow           # Docker image với Chrome + Spark
├── plugins/                     # Custom plugins (nếu cần)
└── logs/                        # Logs từ Airflow
```

## 🚀 Triển khai trên EC2

### Bước 1: Clone repository và chuẩn bị

```bash
# Clone repo (nếu chưa có)
git clone https://github.com/your-repo/bigdata-project.git
cd bigdata-project

# Tạo file .env với AWS credentials
cat > .env << 'EOF'
# AWS S3 Configuration
AWS_S3_BUCKET=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=ap-southeast-1
AWS_S3_PREFIX=premier_league

# Selenium
HEADLESS=true
REQUEST_DELAY=2
EOF
```

### Bước 2: Set Airflow UID (quan trọng!)

```bash
# Tạo file .env cho Airflow
echo "AIRFLOW_UID=$(id -u)" >> airflow/.env
```

### Bước 3: Khởi động Airflow

```bash
cd airflow

# Build và khởi động
sudo docker-compose -f docker-compose.airflow.yml up -d

# Xem logs để đảm bảo khởi động thành công
sudo docker-compose -f docker-compose.airflow.yml logs -f
```

### Bước 4: Truy cập Airflow UI

**URL**: `http://<EC2-PUBLIC-IP>:8081`

**Login credentials**:
- Username: `admin`
- Password: `admin`

## 📊 Các DAGs có sẵn

### 1. `premier_league_batch_scrape` (Daily)

- **Schedule**: Hàng ngày lúc 6:00 AM (UTC+7)
- **Mục đích**: Cào dữ liệu mùa giải hiện tại và chạy ETL
- **Tasks**:
  1. `scrape_current_season`: Cào dữ liệu từ website
  2. `etl_bronze_to_silver`: Chuyển đổi Bronze → Silver
  3. `etl_silver_to_gold`: Chuyển đổi Silver → Gold
  4. `cleanup_temp_files`: Dọn dẹp file tạm
  5. `notify_success`: Log thông báo hoàn thành

### 2. `premier_league_backfill` (Manual)

- **Schedule**: Trigger thủ công
- **Mục đích**: Backfill dữ liệu các mùa giải cũ
- **Cách sử dụng**:
  1. Vào Airflow UI → DAGs → `premier_league_backfill`
  2. Click "Trigger DAG w/ config"
  3. Nhập config JSON:
     ```json
     {
       "seasons": ["2024/25", "2023/24", "2022/23"],
       "workers": 3
     }
     ```
  4. Click "Trigger"

## 🔧 Quản lý Airflow

### Xem trạng thái containers

```bash
cd airflow
sudo docker-compose -f docker-compose.airflow.yml ps
```

### Xem logs

```bash
# Tất cả logs
sudo docker-compose -f docker-compose.airflow.yml logs -f

# Logs của scheduler (quan trọng)
sudo docker logs -f airflow-scheduler

# Logs của webserver
sudo docker logs -f airflow-webserver
```

### Restart services

```bash
cd airflow
sudo docker-compose -f docker-compose.airflow.yml restart

# Hoặc restart từng service
sudo docker-compose -f docker-compose.airflow.yml restart airflow-scheduler
```

### Dừng Airflow

```bash
cd airflow
sudo docker-compose -f docker-compose.airflow.yml down
```

### Xóa hoàn toàn (bao gồm database)

```bash
cd airflow
sudo docker-compose -f docker-compose.airflow.yml down -v
```

## 🛠️ Tùy chỉnh Schedule

Mở file `airflow/dags/batch_scrape_dag.py` và sửa:

```python
# Chạy lúc 6:00 AM mỗi ngày
schedule_interval='0 6 * * *'

# Chạy mỗi 12 tiếng
schedule_interval='0 */12 * * *'

# Chạy vào 8:00 AM thứ 2 mỗi tuần
schedule_interval='0 8 * * 1'

# Cron expressions:
# ┌──────────── minute (0 - 59)
# │ ┌────────── hour (0 - 23)
# │ │ ┌──────── day of month (1 - 31)
# │ │ │ ┌────── month (1 - 12)
# │ │ │ │ ┌──── day of week (0 - 6, 0=Sunday)
# │ │ │ │ │
# * * * * *
```

## 📧 Cấu hình Email Notifications (Tùy chọn)

Thêm vào `docker-compose.airflow.yml`:

```yaml
environment:
  AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
  AIRFLOW__SMTP__SMTP_PORT: 587
  AIRFLOW__SMTP__SMTP_USER: your-email@gmail.com
  AIRFLOW__SMTP__SMTP_PASSWORD: your-app-password
  AIRFLOW__SMTP__SMTP_MAIL_FROM: your-email@gmail.com
```

Cập nhật `default_args` trong DAG:

```python
default_args = {
    'email': ['your-email@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    # ...
}
```

## ⚠️ Yêu cầu hệ thống

- **RAM**: Tối thiểu 4GB (EC2 t2.medium hoặc lớn hơn)
- **Storage**: 20GB+ cho Docker images và logs
- **Ports**: Mở port 8081 trong Security Group

## 🔍 Troubleshooting

### DAG không hiển thị trong UI

```bash
# Kiểm tra syntax DAG
sudo docker exec -it airflow-scheduler python /opt/airflow/dags/batch_scrape_dag.py

# Refresh DAGs
sudo docker exec -it airflow-scheduler airflow dags list
```

### Lỗi database connection

```bash
# Kiểm tra PostgreSQL
sudo docker exec -it airflow-postgres psql -U airflow -c "SELECT 1;"

# Reset database
cd airflow
sudo docker-compose -f docker-compose.airflow.yml down -v
sudo docker-compose -f docker-compose.airflow.yml up -d
```

### Lỗi Chrome/Selenium trong task

```bash
# Kiểm tra Chrome trong container
sudo docker exec -it airflow-scheduler google-chrome --version

# Kiểm tra shm_size
sudo docker inspect airflow-scheduler | grep ShmSize
```

### Task bị timeout

Tăng `execution_timeout` trong DAG:

```python
default_args = {
    'execution_timeout': timedelta(hours=4),  # Tăng lên 4 giờ
}
```

## 📈 Monitoring

### Trong Airflow UI

1. **DAGs Page**: Xem tổng quan tất cả DAGs
2. **Graph View**: Xem flow của DAG
3. **Tree View**: Xem lịch sử chạy theo thời gian
4. **Gantt View**: Xem timeline execution

### Xem logs của từng task

1. Click vào DAG → Click vào ngày chạy
2. Click vào task cụ thể
3. Click "Log" để xem output

## 🔗 Quick Links

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://<EC2-IP>:8081 | admin/admin |
| Spark UI (khi chạy) | http://<EC2-IP>:4040 | - |
| Kafka UI (nếu có) | http://<EC2-IP>:8080 | - |
