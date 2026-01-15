# 🎯 Streaming Architecture & UI Access

## 📊 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DOCKER ENVIRONMENT                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌────────────┐      ┌────────┐      ┌──────────────────────┐      │
│  │ Zookeeper  │◄─────┤ Kafka  │◄─────┤    Scraper           │      │
│  │ :2181      │      │ :9092  │      │ (Match Data Producer)│      │
│  └────────────┘      └───┬────┘      └──────────────────────┘      │
│                          │                                           │
│                          │ Topic: raw-match-data                    │
│                          │                                           │
│                    ┌─────┴──────┐                                   │
│                    │            │                                    │
│                    ▼            ▼                                    │
│         ┌──────────────┐  ┌──────────────┐                         │
│         │ streaming-etl│  │kafka-consumer│                         │
│         │  :4040       │  │  :4041       │                         │
│         │  (Streaming) │  │  (Batch)     │                         │
│         └──────┬───────┘  └──────┬───────┘                         │
│                │                 │                                   │
│                │                 │                                   │
│         Spark UI ✅             Spark UI                           │
│         ALWAYS ON!              When ETL runs                       │
│                │                 │                                   │
│                ▼                 ▼                                   │
│         ┌────────────────────────────┐                             │
│         │      AWS S3 (MinIO)        │                             │
│         │  silver/streaming/         │                             │
│         │  silver/batch/             │                             │
│         └────────────────────────────┘                             │
│                                                                       │
│  ┌────────────┐                                                      │
│  │  kafka-ui  │  Monitor all Kafka activity                         │
│  │  :8080     │  Topics, Messages, Consumers                        │
│  └────────────┘  ✅ ALWAYS ON!                                      │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘

        ▼                                    ▼
┌──────────────────┐             ┌──────────────────┐
│   Browser UI     │             │   Browser UI     │
│                  │             │                  │
│  Kafka UI        │             │  Spark UI        │
│  localhost:8080  │             │  localhost:4040  │
│                  │             │                  │
│  ✅ 24/7 Access  │             │  ✅ 24/7 Access  │
└──────────────────┘             └──────────────────┘
```

---

## 🔄 Data Flow

```
┌──────────────┐
│  Web Scraper │  Scrape Premier League matches
└──────┬───────┘
       │
       │ Selenium + BeautifulSoup
       │
       ▼
┌─────────────────────────┐
│   Match Data (JSON)     │
│   - match_id            │
│   - season              │
│   - matchweek           │
│   - teams, scores       │
│   - lineups, events     │
│   - statistics          │
└──────┬──────────────────┘
       │
       │ KafkaProducer.send()
       │
       ▼
┌─────────────────────────┐
│  Kafka Topic            │
│  "raw-match-data"       │
│  Partitions: 3          │
│  Retention: 7 days      │
└──────┬──────────────────┘
       │
       │ Consumed by
       │
       ├─────────────────────────┐
       │                         │
       ▼                         ▼
┌──────────────────┐    ┌─────────────────┐
│ Streaming ETL    │    │  Batch Consumer │
│ (Real-time)      │    │  (Archive)      │
└──────┬───────────┘    └─────┬───────────┘
       │                      │
       │ Spark Streaming      │ Batch Spark
       │ Micro-batches        │ Full ETL
       │ Every 30s            │ On trigger
       │                      │
       │                      ├─→ Bronze (S3 JSON)
       │                      │
       ▼                      ▼
┌────────────────────────────────┐
│     Silver Layer (S3)          │
│                                │
│  ├─ streaming/                 │
│  │  └─ season=2025-26/         │
│  │     └─ matchweek=1/         │
│  │        └─ *.parquet         │
│  │                             │
│  └─ batch/                     │
│     └─ 2025-26/                │
│        └─ matchweek_table.csv  │
└────────────────────────────────┘
```

---

## 🖥️ UI Access Flow

### Kafka UI Access

```
Browser → http://localhost:8080
              │
              ▼
      ┌───────────────┐
      │   Kafka UI    │
      │   Container   │
      └───────┬───────┘
              │
              │ Connect to
              │
              ▼
      ┌───────────────┐
      │ Kafka Broker  │
      │  (Port 9093)  │
      └───────────────┘
              │
              │ Query
              │
              ▼
      ┌─────────────────────┐
      │  Topics & Messages  │
      │  Consumer Groups    │
      │  Broker Metrics     │
      └─────────────────────┘

Status: ✅ Always available when Kafka running
```

### Spark Streaming UI Access

```
Browser → http://localhost:4040
              │
              ▼
      ┌────────────────────┐
      │ streaming-etl      │
      │ Container          │
      └────────┬───────────┘
               │
               │ Spark Session Created
               │
               ▼
      ┌────────────────────┐
      │ UI Initialization  │  ← NEW! Force init with dummy DF
      │ dummy_df.count()   │
      └────────┬───────────┘
               │
               ▼
      ┌────────────────────┐
      │ Spark UI Server    │
      │ Port 4040          │
      │ ✅ READY!          │
      └────────┬───────────┘
               │
               │ Streaming Query Starts
               │
               ▼
      ┌──────────────────────┐
      │  Kafka → Transform   │
      │  → Write to S3       │
      └──────────────────────┘
               │
               │ Micro-batches (every 30s)
               │
               ▼
      ┌──────────────────────┐
      │ UI Shows:            │
      │ • Active queries     │
      │ • Batch history      │
      │ • Input/output rate  │
      │ • Processing metrics │
      └──────────────────────┘

Status: ✅ Always available (after update!)
        NO need to wait for data
```

---

## 📈 Timeline Comparison

### Before Update ❌

```
T+0s    Docker compose up -d streaming-etl
T+10s   ✅ Container started
T+15s   ✅ Spark session created
T+20s   ⏸️  Waiting for data...
        ❌ UI NOT available yet
        
T+60s   Run scraper
T+65s   First message arrives in Kafka
T+70s   Streaming picks up message
T+75s   ✅ UI appears!

Total wait: ~75 seconds to see UI
```

### After Update ✅

```
T+0s    Docker compose up -d streaming-etl
T+10s   ✅ Container started
T+15s   ✅ Spark session created
T+18s   🎯 UI initialization
T+20s   ✅ UI AVAILABLE!
T+25s   Streaming query started
        
(Scraper can run anytime)
T+60s   Run scraper
T+65s   First message arrives
T+70s   UI shows processing

Total wait: ~20 seconds to see UI
Improvement: 3.75x faster!
```

---

## 🎯 Monitoring Dashboard Layout

### Kafka UI Dashboard
```
┌────────────────────────────────────────────────┐
│               KAFKA UI                         │
├────────────────────────────────────────────────┤
│ Cluster: premier-league-cluster                │
│                                                 │
│ Topics (3)                     Messages         │
│ ├─ raw-match-data              1,234   ✅      │
│ ├─ silver-processing           45     ✅      │
│ └─ dlq-failed-messages         0      ✅      │
│                                                 │
│ Consumer Groups (2)            Lag              │
│ ├─ premier_league_streaming    0       ✅      │
│ └─ premier_league_etl_group    12      ⚠️      │
│                                                 │
│ Brokers (1)                    Status           │
│ └─ kafka:9093                  Online  ✅      │
└────────────────────────────────────────────────┘
```

### Spark Streaming UI Dashboard
```
┌────────────────────────────────────────────────┐
│         SPARK STREAMING UI                     │
├────────────────────────────────────────────────┤
│ Application: PremierLeagueStreamingETL         │
│                                                 │
│ STREAMING TAB                                   │
│ ┌────────────────────────────────────────────┐ │
│ │ Active Queries (1)                         │ │
│ │                                            │ │
│ │ Query: raw-match-data → silver/streaming  │ │
│ │ Status: RUNNING ✅                         │ │
│ │                                            │ │
│ │ Input Rate:    5.2 records/sec             │ │
│ │ Process Rate:  6.8 records/sec             │ │
│ │ Batch Duration: 2.3 sec (avg)              │ │
│ │                                            │ │
│ │ Completed Batches:                         │ │
│ │ ├─ Batch 42  [2026-01-14 22:30:00]  ✅    │ │
│ │ ├─ Batch 41  [2026-01-14 22:29:30]  ✅    │ │
│ │ └─ Batch 40  [2026-01-14 22:29:00]  ✅    │ │
│ └────────────────────────────────────────────┘ │
│                                                 │
│ JOBS | STAGES | STORAGE | ENVIRONMENT | SQL    │
└────────────────────────────────────────────────┘
```

---

## 🔧 Configuration Summary

### Services & Ports

| Service | Internal Port | External Port | UI |
|---------|--------------|---------------|-----|
| Zookeeper | 2181 | 2181 | - |
| Kafka | 9093 (internal)<br>9092 (external) | 9092 | - |
| Kafka UI | 8080 | 8080 | ✅ |
| Streaming ETL | 4040 | 4040 | ✅ |
| Batch Consumer | 4040 | 4041 | ⏸️ |

### Environment Variables

```yaml
streaming-etl:
  environment:
    - KAFKA_BOOTSTRAP_SERVERS=kafka:9093
    - SPARK_DRIVER_MEMORY=4g
    - JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    - SPARK_LOCAL_IP=127.0.0.1
```

### Spark UI Configs (New)

```python
.config("spark.ui.enabled", "true")
.config("spark.ui.port", "4040")
.config("spark.ui.host", "0.0.0.0")
.config("spark.ui.retainedJobs", "1000")
.config("spark.ui.retainedStages", "1000")
.config("spark.streaming.ui.retainedBatches", "1000")
```

---

**Visual guides complete! See STREAMING_UI_GUIDE.md for usage.** 📊
