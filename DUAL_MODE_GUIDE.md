# 🌊 Dual-Mode Architecture: Batch + Streaming

## 🎯 Overview

Hệ thống hỗ trợ **2 luồng xử lý độc lập**:

### **Luồng 1: BATCH Processing** (With Bronze)
```
Scraper → Bronze (S3) → Manual Spark ETL → Silver (S3)
          ↓
    JSON Archive
    Long-term storage
    Historical data
```

### **Luồng 2: STREAMING Processing** (NO Bronze) ⚡ NEW!
```
Scraper → Kafka → Spark Streaming → Silver (S3)
                   ↓
              Real-time
              Direct processing
              NO intermediate storage
```

---

## 📊 Architecture Diagram

```
                    ┌─────────────┐
                    │   SCRAPER   │
                    └──────┬──────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
      ┌──────────────┐          ┌─────────────┐
      │   KAFKA      │          │  Bronze(S3) │
      │   Topics     │          │   (JSON)    │
      └──────┬───────┘          └──────┬──────┘
             │                         │
             │                         ▼
             │                  ┌─────────────┐
             │                  │Manual Spark │
             │                  │    ETL      │
             │                  └──────┬──────┘
             │                         │
             ▼                         │
      ┌──────────────┐                │
      │Spark Streaming│               │
      │  (Real-time) │                │
      └──────┬───────┘                │
             │                         │
             └────────┬────────────────┘
                      ▼
              ┌──────────────┐
              │  Silver (S3) │
              │    (CSV)     │
              └──────────────┘
```

---

## 🚀 Quick Start

### Start Streaming Mode (NEW - Recommended)

```bash
# 1. Start infrastructure
docker-compose up -d zookeeper kafka kafka-ui

# 2. Start Spark Streaming (Real-time processing)
docker-compose up -d streaming-etl

# 3. Run scraper (sends to Kafka)
docker-compose up scraper

# 4. Monitor
http://localhost:8080  # Kafka UI
http://localhost:4040  # Spark Streaming UI
```

### Start Batch Mode (Original)

```bash
# 1. Start infrastructure
docker-compose up -d zookeeper kafka kafka-ui

# 2. Start consumer (Kafka → Bronze → Silver)
docker-compose --profile batch up -d kafka-consumer

# 3. Run scraper
docker-compose up scraper

# 4. Monitor
http://localhost:8080  # Kafka UI
http://localhost:4041  # Batch Spark UI
```

---

## 📋 Services Overview

| Service | Port | Purpose | Mode |
|---------|------|---------|------|
| **streaming-etl** | 4040 | Spark Streaming (Kafka→Silver) | Streaming |
| **kafka-consumer** | 4041 | Batch processing (Kafka→Bronze→Silver) | Batch |
| **kafka-ui** | 8080 | Kafka monitoring | Both |
| **kafka** | 9092 | Message broker | Both |

---

## 🔄 Data Flow Comparison

### Streaming Flow (Real-time):
```
1. Scraper crawls match
2. Send to Kafka (raw-match-data topic)
3. Spark Streaming reads from Kafka
4. Transform & flatten inline
5. Write directly to Silver (S3)
   ↓
✅ Real-time (seconds)
✅ NO intermediate storage
✅ Lower latency
```

### Batch Flow (Historical):
```
1. Scraper crawls match
2. Send to Kafka (raw-match-data topic)
3. Consumer reads from Kafka
4. Upload to Bronze (S3 JSON)
5. Trigger Silver ETL
6. Spark reads Bronze
7. Transform to Silver (S3 CSV)
   ↓
✅ Raw data preserved
✅ Can replay/audit
✅ Historical archive
```

---

## 🎯 When to Use Which

### Use STREAMING when:
```
✅ Need real-time updates
✅ Don't need raw data archive
✅ Want lowest latency
✅ Processing simple transformations
✅ High throughput required
```

### Use BATCH when:
```
✅ Need historical raw data
✅ Compliance/audit requirements
✅ Complex transformations
✅ Data debugging needed
✅ Multiple downstream consumers of raw data
```

---

## 💻 Commands

### Streaming Mode:

```bash
# Start streaming
docker-compose up -d streaming-etl

# View logs
docker-compose logs -f streaming-etl

# Spark UI
http://localhost:4040

# Stop
docker-compose stop streaming-etl
```

### Batch Mode:

```bash
# Start consumer (with profile)
docker-compose --profile batch up -d kafka-consumer

# View logs
docker-compose logs -f kafka-consumer

# Spark UI (when ETL runs)
http://localhost:4041

# Stop
docker-compose stop kafka-consumer
```

### Run Both Modes:

```bash
# Start both streaming AND batch
docker-compose --profile batch up -d streaming-etl kafka-consumer

# Scraper sends to Kafka
# → Streaming processes real-time → Silver
# → Batch archives to Bronze → Silver

# Result: Bronze + Silver populated!
```

---

## 📊 Output Structure

### Streaming Output:
```
s3://bucket/premier_league/silver/streaming/
├── season=2025-26/
│   ├── matchweek=1/
│   │   ├── part-00000.parquet
│   │   └── part-00001.parquet
│   └── matchweek=2/
│       └── part-00000.parquet
```

### Batch Output:
```
s3://bucket/premier_league/
├── bronze/
│   └── 2025-26/
│       └── matchweek_01/
│           ├── match_123.json
│           └── match_124.json
└── silver/
    └── 2025-26/
        ├── matchweek_table_*.csv
        └── season_table_*.csv
```

---

## 🔍 Monitoring

### Kafka UI (Both modes)
```
URL: http://localhost:8080

Topics:
- raw-match-data (incoming matches)
- silver-processing (batch triggers)

Consumer Groups:
- streaming: premier_league_streaming (Spark Structured Streaming)
- batch: premier_league_etl_group (Consumer)
```

### Spark UI

**Streaming Spark UI:**
```
URL: http://localhost:4040

Shows:
- Streaming queries
- Micro-batch processing
- Input rate (records/sec)
- Processing time
- Kafka source metrics
```

**Batch Spark UI:**
```
URL: http://localhost:4041

Shows:
- ETL jobs
- Stage details
- Task metrics
- When triggered by consumer
```

---

## 🛠️ Configuration

### Streaming ETL Config:

```yaml
streaming-etl:
  command: python streaming_etl.py --kafka-servers kafka:9093 --format parquet
  environment:
    - SPARK_DRIVER_MEMORY=4g
  ports:
    - "4040:4040"  # Spark Streaming UI
```

### Batch Consumer Config:

```yaml
kafka-consumer:
  command: python -m kafka.consumer
  ports:
    - "4041:4040"  # Batch Spark UI
  profiles:
    - batch  # Only starts with: docker-compose --profile batch up
```

---

## ⚡ Performance Comparison

| Metric | Streaming | Batch |
|--------|-----------|-------|
| **Latency** | Seconds | Minutes |
| **Throughput** | High | Medium |
| **Storage** | Silver only | Bronze + Silver |
| **Cost** | Lower | Higher |
| **Audit** | Limited | Full |
| **Replay** | From Kafka retention | From Bronze (permanent) |

---

## 🔧 Troubleshooting

### Streaming not processing?

```bash
# Check Spark Streaming logs
docker-compose logs -f streaming-etl

# Common issues:
# - Kafka not reachable
# - Schema mismatch
# - S3 permissions

# Restart
docker-compose restart streaming-etl
```

### Both modes running - conflicts?

```
NO conflicts! They run independently:

Streaming: Port 4040, processes real-time
Batch: Port 4041, archives + processes

Same data → Different outputs:
- Streaming: silver/streaming/
- Batch: bronze/ + silver/batch/
```

---

## 📚 Files

```
streaming_etl.py        # NEW - Spark Structured Streaming
kafka/consumer.py       # Existing - Batch consumer
docker-compose.yml      # Updated - Added streaming-etl service
DUAL_MODE_GUIDE.md      # This file
```

---

## 🎯 Recommended Setup

### Development:
```bash
# Use Batch mode (easier to debug)
docker-compose --profile batch up -d
```

### Production:
```bash
# Use Streaming mode (real-time)
docker-compose up -d streaming-etl

# Optional: Also run batch for archival
docker-compose --profile batch up -d kafka-consumer
```

### Best of Both:
```bash
# Run BOTH modes
docker-compose --profile batch up -d streaming-etl kafka-consumer

Benefits:
✅ Real-time Silver (streaming)
✅ Historical Bronze (batch)
✅ Audit trail
✅ Can replay from Bronze if needed
```

---

## ✅ Summary

**You now have 2 independent pipelines:**

1. **Streaming** (Real-time): Kafka → Silver ⚡
2. **Batch** (Historical): Kafka → Bronze → Silver 📦

**Choose based on needs:**
- Need speed? → Streaming
- Need history? → Batch
- Need both? → Run both! 🚀

---

**Next: Run `docker-compose up -d streaming-etl` to start streaming!**
