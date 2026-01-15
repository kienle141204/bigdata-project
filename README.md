# Premier League Data Pipeline

Pipeline xử lý dữ liệu bóng đá Premier League với kiến trúc Medallion (Bronze-Silver-Gold).

## 🏗️ Kiến trúc

### Dual Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PREMIER LEAGUE DATA PIPELINE                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  📦 BATCH PIPELINE (Airflow)           🌊 STREAMING PIPELINE        │
│  ┌─────────────────────────┐           ┌─────────────────────────┐  │
│  │ Scraper → Bronze → S3  │           │ Producer → Kafka →      │  │
│  │     ↓                   │           │     Spark Streaming →   │  │
│  │ Silver (CSV Tables)     │           │     Iceberg Tables      │  │
│  │     ↓                   │           │                         │  │
│  │ Gold (Team-specific)    │           └─────────────────────────┘  │
│  └─────────────────────────┘                                        │
│                                                                      │
│  ⏰ Schedule: Daily @ 6AM              ⚡ Real-time processing       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 3 Layers

```
Bronze (JSON)  →  Silver (CSV Tables)  →  Gold (Cleaned)
  Raw Data         3 types of tables      Team-specific

### Bronze Layer
- **Format**: JSON
- **Content**: Dữ liệu thô từ scraper
- **Path**: `s3://bucket/premier-league/bronze/{season}/{matchweek}/`
- **Ví dụ**: `bronze/2011-12/01/match_360486.json`

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
```

### Chạy Pipeline

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
├── scraper/             # Web scrapers
│   ├── season_scraper.py
│   └── match_scraper.py
├── pipeline.py          # Main pipeline script
├── scrape_to_s3.py      # Standalone scraper
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

## 🚀 Quick Start Deployment

| Deployment | Guide | Port | Description |
|------------|-------|------|-------------|
| ☸️ **Kubernetes** | [k8s/DEPLOY_K8S.md](./k8s/DEPLOY_K8S.md) | 8080, 8081 | Production-ready K8s deployment với auto-scaling |
| 📦 **Batch (Airflow)** | [DEPLOY_AIRFLOW.md](./DEPLOY_AIRFLOW.md) | 8081 | Daily scheduled scraping with Airflow UI |
| 🌊 **Streaming** | [DEPLOY_STREAMING.md](./DEPLOY_STREAMING.md) | 8080, 4040 | Real-time Kafka + Spark Streaming |
| ⚡ **Batch (Docker)** | [DEPLOY_BATCH.md](./DEPLOY_BATCH.md) | - | Simple Docker-based batch processing |

**💡 Không chắc có cần Kubernetes?** Xem [k8s/K8S_VS_DOCKER_COMPOSE.md](./k8s/K8S_VS_DOCKER_COMPOSE.md) để hiểu khi nào cần K8s và khi nào không!

### Airflow (Khuyến nghị cho Production)

```bash
cd airflow
chmod +x start-airflow.sh
./start-airflow.sh

# Access UI: http://<EC2-IP>:8081
# Login: admin / admin
```

## 📝 License

MIT
