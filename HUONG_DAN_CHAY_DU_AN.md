# 🚀 Hướng Dẫn Chạy Dự Án Premier League Data Pipeline

Dự án này xử lý dữ liệu bóng đá Premier League với kiến trúc Medallion (Bronze-Silver-Gold) và hỗ trợ cả Batch và Streaming processing.

---

## 📋 Mục Lục

1. [Yêu Cầu Hệ Thống](#yêu-cầu-hệ-thống)
2. [Cài Đặt](#cài-đặt)
3. [Cấu Hình](#cấu-hình)
4. [Cách Chạy Dự Án](#cách-chạy-dự-án)
   - [Cách 1: Chạy với Docker (Khuyến nghị)](#cách-1-chạy-với-docker-khuyến-nghị)
   - [Cách 2: Chạy trực tiếp với Python](#cách-2-chạy-trực-tiếp-với-python)
   - [Cách 3: Chạy với Airflow (Production)](#cách-3-chạy-với-airflow-production)
5. [Các Luồng Xử Lý](#các-luồng-xử-lý)
6. [Troubleshooting](#troubleshooting)

---

## 📦 Yêu Cầu Hệ Thống

### Tối thiểu:
- **Docker Desktop** (khuyến nghị) hoặc Python 3.8+
- **RAM**: Tối thiểu 4GB (khuyến nghị 8GB cho Docker)
- **Ổ cứng**: Ít nhất 10GB trống
- **AWS Account** với S3 bucket và credentials

### Nếu chạy trực tiếp (không dùng Docker):
- Python 3.8+
- Java 17+ (cho Spark)
- Chrome/Chromium browser
- Các thư viện Python (xem `requirements.txt`)

---

## 🔧 Cài Đặt

### Option 1: Sử dụng Docker (Khuyến nghị)

1. **Cài đặt Docker Desktop**
   - Windows: Tải từ [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)
   - Đảm bảo Docker Desktop đang chạy

2. **Build Docker images**
   ```bash
   docker-compose build
   ```

### Option 2: Cài đặt trực tiếp

```bash
# Cài đặt Python dependencies
pip install -r requirements.txt

# Cài đặt Java (cho Spark)
# Windows: Tải từ oracle.com hoặc dùng OpenJDK
# Linux/Mac: sudo apt-get install openjdk-17-jdk
```

---

## ⚙️ Cấu Hình

### Bước 1: Tạo file `.env`

Tạo file `.env` ở thư mục gốc của dự án:

```env
# AWS S3 Configuration (BẮT BUỘC)
AWS_S3_BUCKET=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
AWS_REGION=ap-southeast-1
AWS_S3_PREFIX=premier_league

# Selenium Configuration
HEADLESS=true
REQUEST_DELAY=2

# Kafka Configuration (cho Streaming)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_RAW=premier_league_raw
```

### Bước 2: Kiểm tra cấu hình

```bash
# Kiểm tra file .env đã tồn tại
cat .env

# Kiểm tra AWS credentials
aws s3 ls s3://your-bucket-name/
```

---

## 🎯 Cách Chạy Dự Án

### Cách 1: Chạy với Docker (Khuyến nghị)

Docker tự động quản lý tất cả dependencies, không cần cài đặt thủ công.

#### A. Batch Processing (Luồng xử lý theo lô)

**Bước 1: Cào dữ liệu (Bronze Layer)**
```bash
# Cào vòng 1 mùa 2011/12 và lưu JSON vào S3 Bronze
docker-compose run --rm scraper python scrape_to_s3.py --matchweek 1 --season "2011/12" --workers 1

# Cào nhiều vòng đấu
docker-compose run --rm scraper python scrape_to_s3.py --matchweek 1 2 3 --season "2011/12" --workers 2

# Cào toàn bộ mùa giải (38 vòng đấu)
docker-compose run --rm scraper python scrape_to_s3.py --matchweek $(seq 1 38) --season "2011/12" --workers 5
```

**Bước 2: Xử lý ETL (Silver & Gold Layers)**
```bash
# Chạy toàn bộ pipeline: Bronze -> Silver CSV -> Gold CSV
docker-compose run --rm spark-job python pipeline.py --seasons "2011/12" --create-gold

# Chỉ tạo lại Gold layer (nếu Silver đã có sẵn)
docker-compose run --rm spark-job python pipeline.py --gold-only

# Xử lý nhiều mùa giải
docker-compose run --rm spark-job python pipeline.py --seasons "2011/12" "2012/13" "2013/14" --create-gold
```

#### B. Streaming Processing (Luồng xử lý thời gian thực)

**Bước 1: Khởi động các dịch vụ**
```bash
# Chạy Kafka, Zookeeper, Spark Streaming
docker-compose up -d zookeeper kafka kafka-ui spark-streaming

# Kiểm tra các dịch vụ đã chạy
docker-compose ps
```

**Bước 2: Truy cập UI**
- **Kafka UI**: http://localhost:8080
- **Spark UI**: http://localhost:4040

**Bước 3: Gửi dữ liệu vào Kafka**
```bash
# Cào dữ liệu và đẩy vào Kafka
docker-compose run --rm scraper python kafka/producer.py --matchweek 1 --season "2011/12"
```

**Bước 4: Đồng bộ sang Gold Layer**
```bash
# Xuất dữ liệu từ Iceberg sang CSV
docker-compose run --rm spark-job python kafka/silver_to_gold_iceberg.py
```

**Bước 5: Dừng các dịch vụ**
```bash
docker-compose down
```

---

### Cách 2: Chạy trực tiếp với Python

#### A. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

#### B. Chạy Batch Pipeline

```bash
# Cào dữ liệu
python scrape_to_s3.py --matchweek 1 --season "2011/12" --workers 1

# Chạy ETL pipeline
python pipeline.py --seasons "2011/12" --create-gold

# Xử lý tất cả mùa giải
python pipeline.py --all-seasons
```

#### C. Chạy Streaming Pipeline

```bash
# Terminal 1: Khởi động Spark Streaming
python kafka/spark_consumer.py

# Terminal 2: Gửi dữ liệu vào Kafka
python kafka/producer.py --matchweek 1 --season "2011/12"

# Terminal 3: Đồng bộ sang Gold
python kafka/silver_to_gold_iceberg.py
```

---

### Cách 3: Chạy với Airflow (Production)

Airflow cho phép lên lịch tự động và quản lý workflow tốt hơn.

#### Bước 1: Khởi động Airflow

```bash
cd airflow
chmod +x start-airflow.sh
./start-airflow.sh
```

#### Bước 2: Truy cập Airflow UI

- **URL**: http://localhost:8081 (hoặc http://<EC2-IP>:8081 nếu deploy trên EC2)
- **Username**: admin
- **Password**: admin

#### Bước 3: Kích hoạt DAGs

1. Vào Airflow UI
2. Tìm các DAG: `batch_scrape_dag`, `backfill_dag`
3. Bật toggle để kích hoạt DAG
4. DAG sẽ tự động chạy theo lịch (mặc định: 6:00 AM hàng ngày)

#### Bước 4: Dừng Airflow

```bash
cd airflow
./stop-airflow.sh
```

Chi tiết hơn xem: [DEPLOY_AIRFLOW.md](./DEPLOY_AIRFLOW.md)

---

## 🔄 Các Luồng Xử Lý

### 1. Batch Pipeline (Bronze → Silver → Gold)

```
Scraper → Bronze (JSON) → Silver (CSV Tables) → Gold (Team-specific CSV)
```

**Bronze Layer:**
- Format: JSON
- Path: `s3://bucket/premier_league/bronze/{season}/{matchweek}/`

**Silver Layer:**
- Format: CSV
- 3 loại bảng:
  - Matchweek Table: `silver/{season}/matchweek_table_*.csv`
  - Season Table: `silver/{season}/season_table_*.csv`
  - Master Table: `silver/master/MASTER_ALL_SEASONS_*.csv`

**Gold Layer:**
- Format: CSV
- Path: `s3://bucket/premier_league/gold/{Team_Name}/`

### 2. Streaming Pipeline (Kafka → Iceberg → Gold)

```
Producer → Kafka → Spark Streaming → Iceberg Tables → Gold CSV
```

**Kafka Topics:**
- `premier_league_raw`: Dữ liệu thô từ scraper

**Iceberg Tables:**
- Silver layer: Bảng Iceberg trên S3
- Gold layer: CSV files từ Iceberg

---

## 🐛 Troubleshooting

### Lỗi: "Chromium crashed" hoặc "Chrome crashed"

**Nguyên nhân:** Thiếu RAM hoặc shared memory

**Giải pháp:**
```bash
# Giảm số workers
docker-compose run --rm scraper python scrape_to_s3.py --matchweek 1 --season "2011/12" --workers 1

# Hoặc tăng RAM cho Docker Desktop (Settings → Resources → Memory)
```

### Lỗi: "AWS credentials not found"

**Giải pháp:**
```bash
# Kiểm tra file .env
cat .env

# Đảm bảo có các biến:
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# AWS_S3_BUCKET=...
```

### Lỗi: "Kafka connection refused"

**Giải pháp:**
```bash
# Kiểm tra Kafka đã chạy chưa
docker-compose ps

# Khởi động lại Kafka
docker-compose up -d kafka zookeeper

# Kiểm tra logs
docker-compose logs kafka
```

### Lỗi: "PySpark not found"

**Giải pháp:**
```bash
pip install pyspark>=3.5.0

# Hoặc dùng Docker (khuyến nghị)
docker-compose run --rm spark-job python pipeline.py --seasons "2011/12"
```

### Lỗi: "No data in Bronze"

**Giải pháp:**
```bash
# Cào dữ liệu trước khi chạy ETL
docker-compose run --rm scraper python scrape_to_s3.py --matchweek 1 --season "2011/12"
```

### Xem logs

```bash
# Logs của container đang chạy
docker-compose logs -f spark-streaming

# Logs của container đã dừng
docker-compose logs scraper

# Logs từ file
cat logs/pipeline_*.log
```

---

## 📊 Kiểm Tra Kết Quả

### Kiểm tra dữ liệu trên S3

```bash
# List Bronze files
aws s3 ls s3://your-bucket/premier_league/bronze/ --recursive

# List Silver files
aws s3 ls s3://your-bucket/premier_league/silver/ --recursive

# List Gold files
aws s3 ls s3://your-bucket/premier_league/gold/ --recursive
```

### Kiểm tra Kafka topics

```bash
# Vào Kafka UI: http://localhost:8080
# Hoặc dùng kafka-console-consumer
docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic premier_league_raw --from-beginning
```

---

## 🎓 Ví Dụ Hoàn Chỉnh

### Ví dụ 1: Cào và xử lý 1 mùa giải

```bash
# 1. Cào dữ liệu (Bronze)
docker-compose run --rm scraper python scrape_to_s3.py \
  --matchweek $(seq 1 38) \
  --season "2011/12" \
  --workers 3

# 2. Chạy ETL (Silver & Gold)
docker-compose run --rm spark-job python pipeline.py \
  --seasons "2011/12" \
  --create-gold

# 3. Kiểm tra kết quả
aws s3 ls s3://your-bucket/premier_league/gold/ --recursive
```

### Ví dụ 2: Streaming với Kafka

```bash
# 1. Khởi động services
docker-compose up -d zookeeper kafka kafka-ui spark-streaming

# 2. Gửi dữ liệu vào Kafka
docker-compose run --rm scraper python kafka/producer.py \
  --matchweek 1 2 3 \
  --season "2011/12"

# 3. Đồng bộ sang Gold
docker-compose run --rm spark-job python kafka/silver_to_gold_iceberg.py

# 4. Dừng services
docker-compose down
```

---

## 📚 Tài Liệu Tham Khảo

- [README.md](./README.md) - Tổng quan dự án
- [DOCKER_GUIDE.md](./DOCKER_GUIDE.md) - Hướng dẫn Docker chi tiết
- [DEPLOY_AIRFLOW.md](./DEPLOY_AIRFLOW.md) - Deploy với Airflow
- [DEPLOY_STREAMING.md](./DEPLOY_STREAMING.md) - Deploy Streaming Pipeline
- [DEPLOY_BATCH.md](./DEPLOY_BATCH.md) - Deploy Batch Pipeline

---

## 💡 Tips

1. **Luôn dùng Docker** để tránh vấn đề về dependencies
2. **Bắt đầu với 1 worker** để test, sau đó tăng dần
3. **Kiểm tra logs** thường xuyên để phát hiện lỗi sớm
4. **Backup dữ liệu** trên S3 trước khi chạy ETL lớn
5. **Sử dụng Airflow** cho production để có lịch trình tự động

---

**Chúc bạn chạy dự án thành công! 🎉**
