# Premier League Data Pipeline

Pipeline xử lý dữ liệu bóng đá Premier League với kiến trúc Medallion (Bronze-Silver-Gold).

## 🏗️ Kiến trúc

### Dual-Mode Architecture ⚡ NEW!

Hệ thống hỗ trợ **2 luồng xử lý độc lập**:

#### **Luồng 1: STREAMING** (Real-time) ⚡
```
Scraper → Kafka → Spark Streaming → Silver (S3)
                   ↓
              Real-time processing
              NO Bronze layer
              Lowest latency
```

#### **Luồng 2: BATCH** (Historical) 📦
```
Scraper → Kafka → Consumer → Bronze (S3) → Spark ETL → Silver (S3)
                               ↓
                         Raw data archive
                         Audit trail
```

**Choose based on needs:**
- 🚀 Need real-time? → Streaming
- 📚 Need history? → Batch  
- 🎯 Need both? → Run both!

### Medallion Architecture (3 Layers)

### Bronze Layer (Batch mode only)
- **Format**: JSON
- **Content**: Dữ liệu thô từ scraper (archive)
- **Path**: `s3://bucket/premier-league/bronze/{season}/{matchweek}/`
- **Ví dụ**: `bronze/2025-26/01/match_2561896.json`

### Silver Layer
- **Format**: CSV
- **Content**: 3 loại bảng tổng hợp
- **Path**: `s3://bucket/premier-league/silver/`

#### 3 loại bảng trong Silver:

**1. Bảng vòng (Matchweek Table)**
- Theo từng mùa
- Cột: `match_id`, `matchweek`
- Path: `silver/{season}/matchweek_table_*.csv`

**2. Bảng mùa (Season Table)**
- Theo từng mùa
- Cột: `match_id`, `matchweek`, `season`
- Path: `silver/{season}/season_table_*.csv`

**3. Bảng tổng hợp (Master Table)**
- Tất cả các mùa
- Cột: `match_id`, `matchweek`, `season`
- Path: `silver/master/MASTER_ALL_SEASONS_*.csv`

### Gold Layer
- **Status**: Chưa implement
- **Mục đích**: Dữ liệu đã làm sạch từ Silver

## 🚀 Sử dụng

### Cài đặt

```bash
pip install -r requirements.txt
```

Yêu cầu:
- Python 3.8+
- Apache Spark (PySpark)
- AWS S3 Access

### Cấu hình

Tạo file `.env`:
```env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_S3_BUCKET=your_bucket
AWS_REGION=us-east-1

# Kafka (optional, for Kafka mode)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
USE_KAFKA=true
```

## 🔥 Quick Start with Kafka

### Option 1: Using Start Script (Recommended)

**Windows:**
```powershell
# Start Kafka infrastructure
.\start_kafka.ps1

# Run scraper with Kafka
python scrape_to_s3.py --matchweek 1 --use-kafka
```

**Linux/Mac:**
```bash
# Start Kafka infrastructure
bash start_kafka.sh

# Run scraper with Kafka
python scrape_to_s3.py --matchweek 1 --use-kafka
```

### Option 2: Manual Setup

```bash
# 1. Start Kafka infrastructure
docker-compose up -d zookeeper kafka kafka-ui kafka-consumer

# 2. Create Kafka topics
python -m kafka.topic_manager create

# 3. Run scraper (sends data to Kafka)
python scrape_to_s3.py --matchweek 1 2 3 --workers 3 --use-kafka

# 4. Monitor Kafka UI
# Open http://localhost:8080

# 5. Check consumer logs
docker-compose logs -f kafka-consumer
```

📚 **See [KAFKA_GUIDE.md](KAFKA_GUIDE.md) for detailed Kafka documentation**

## 📋 Traditional Pipeline (Without Kafka)

**1. Xử lý tất cả các mùa (auto discover):**
```bash
python pipeline.py --all-seasons
```

**2. Xử lý mùa cụ thể:**
```bash
python pipeline.py --seasons 2011/12 2012/13
```

**3. Chạy trong Docker:**
```bash
# Build image
docker-compose build

# Xử lý tất cả mùa
docker-compose run --rm spark-job python pipeline.py --all-seasons

# Xử lý mùa cụ thể
docker-compose run --rm spark-job python pipeline.py --seasons 2011/12
```

## 📊 Output

### Ví dụ: Mùa 2011/12 với 352 trận

**Bảng vòng:**
```csv
match_id,matchweek
360486,1
360487,1
360488,2
...
```

**Bảng mùa:**
```csv
match_id,matchweek,season
360486,1,2011/12
360487,1,2011/12
360488,2,2011/12
...
```

**Bảng master (tất cả mùa):**
```csv
match_id,matchweek,season
360486,1,2011/12
360487,1,2011/12
370123,1,2012/13
370124,1,2012/13
...
```

## 🐳 Docker

### Chạy với Docker Compose

```bash
# Xử lý tất cả mùa
docker-compose run --rm spark-job python pipeline.py --all-seasons

# Xử lý 1 mùa
docker-compose run --rm spark-job python pipeline.py --seasons 2011/12
```

### Xem logs

```bash
cat logs/pipeline_*.log
```

## 📁 Cấu trúc

```
.
├── data/
│   ├── etl.py           # ETL Pipeline: Bronze -> Silver
│   ├── processor.py     # S3 Data Store
│   └── db.py            # Database utilities
├── kafka/
│   ├── config.py        # Kafka configuration
│   ├── producer.py      # Kafka producer
│   ├── consumer.py      # Kafka consumer
│   └── topic_manager.py # Topic management utility
├── scraper/             # Web scrapers
│   ├── season_scraper.py
│   └── match_scraper.py
├── pipeline.py          # Main pipeline script
├── scrape_to_s3.py      # Standalone scraper (Kafka-enabled)
├── docker-compose.yml   # Docker services (Kafka, Zookeeper, etc.)
├── start_kafka.ps1      # Windows quick start
├── start_kafka.sh       # Linux/Mac quick start
├── KAFKA_GUIDE.md       # Kafka documentation
└── README.md
```

## 🔧 API

```python
from data.etl import ETLPipeline

# Initialize
etl = ETLPipeline(bucket_name="my-bucket", prefix="premier-league")

# Process all seasons
etl.bronze_to_silver()

# Process specific seasons
etl.bronze_to_silver(seasons=["2011/12", "2012/13"])
```

## 📈 Hiệu năng

- **Scraping**: ~5-10 phút/season (38 matchweeks)
- **Bronze → Silver**: ~30-60 giây/season (với Spark)
- **Total processing**: ~1-2 phút cho 1 season

## ⚡ Workflow

```
1. Scrape dữ liệu
   └─> Bronze (JSON files)

2. Run pipeline
   └─> Silver (3 CSV tables per season + 1 master table)

3. [Future] Gold layer
   └─> Cleaned analytical data
```

## 🐛 Troubleshooting

### Lỗi: PySpark not found
```bash
pip install pyspark
```

### Lỗi: AWS credentials
```bash
# Check .env file
cat .env

# Or set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
```

### Lỗi: No data in Bronze
```bash
# Scrape data first
python scrape_to_s3.py --matchweek 1 --season "2011/12"
```

## 📝 License

MIT
