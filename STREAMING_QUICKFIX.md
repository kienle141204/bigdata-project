# 🔧 Streaming Quick Fix Guide

## ✅ Fixed Issues

### Issue 1: Missing Kafka Connector
**Error:**
```
Failed to find data source: kafka
```

**Fix Applied:**
Added Kafka package to Spark:
```python
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
```

### Issue 2: Missing S3 Connector  
**Error:**
```
ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem
```

**Fix Applied:**
Added Hadoop AWS package:
```python
packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",  # Kafka
    "org.apache.hadoop:hadoop-aws:3.3.4"  # S3
]
```

---

## 🚀 Test Streaming NOW

### 1. Restart service (đã restart rồi)
```bash
docker-compose restart streaming-etl
```

### 2. Wait for initialization (30-60 seconds)
Spark needs to download JARs first time (~50MB)

### 3. Monitor logs
```bash
docker-compose logs -f streaming-etl
```

**Look for:**
```
✅ Spark session created
📡 Connecting to Kafka stream...
✅ Connected to Kafka stream
✅ Streaming query started!
🎯 Spark UI available at: http://localhost:4040
```

### 4. Open Spark UI
```
http://localhost:4040
```

You should see:
- **Streaming** tab
- Active streaming query
- Input rate graph

### 5. Send test data
```bash
# Run scraper in another terminal
docker-compose up scraper

# Or send single match
python scrape_to_s3.py --matchweek 1 --use-kafka
```

---

## 📊 What to Expect

### In Logs:
```
Ivy Default Cache set to: /root/.ivy2/cache
...downloading JARs... (first time only)
24/01/14 15:00:00 INFO SparkSession: Created
24/01/14 15:00:05 INFO KafkaSource: Kafka source started
24/01/14 15:00:10 INFO MicroBatchExecution: Streaming query started
```

### In Spark UI (http://localhost:4040):
- **Streaming Tab**: Shows active query
- **Input Rate**: Messages/second from Kafka
- **Processing Time**: Time per micro-batch
- **Completed Batches**: List of processed batches

### In S3:
```
s3://bucket/premier_league/silver/streaming/
└── season=2025-26/
    └── matchweek=1/
        └── part-00000-xxx.parquet
```

---

## ⏱️ Timeline

```
T+0s:  docker-compose restart streaming-etl
T+10s: Downloading Kafka connector JAR (~30MB)
T+20s: Downloading Hadoop AWS JAR (~20MB)
T+30s: Initializing Spark session
T+40s: Connecting to Kafka
T+50s: Streaming query started ✅
T+60s: Spark UI available at :4040 ✅
```

---

## 🔍 Verify It's Working

### Check 1: Container Running
```bash
docker-compose ps streaming-etl

# Should show: Up
```

### Check 2: Logs Healthy
```bash
docker-compose logs --tail=20 streaming-etl

# Should see: "Streaming query started"
```

### Check 3: Spark UI Accessible
```bash
curl http://localhost:4040

# Should return HTML
```

### Check 4: Kafka Connected
```bash
# In Kafka UI: http://localhost:8080
# Consumer Groups → Should see streaming consumer
```

---

## ⚠️ If Still Not Working

### Check Port 4040
```powershell
# Windows
netstat -ano | findstr :4040

# If occupied, Spark UI won't start
# Change port in docker-compose.yml:
# ports: - "4042:4040"
```

### Increase Wait Time
Spark initialization can take 60-90 seconds first time (downloading JARs)

### View Full Logs
```bash
docker-compose logs streaming-etl | more

# Look for specific error
```

---

## 🎯 Quick Commands

```bash
# Restart
docker-compose restart streaming-etl

# Logs (follow)
docker-compose logs -f streaming-etl

# Check if running
docker-compose ps streaming-etl

# Stop
docker-compose stop streaming-etl

# Start
docker-compose up -d streaming-etl

# Rebuild (if code changed)
docker-compose up -d --build streaming-etl
```

---

## ✅ Success Indicators

When working correctly, you'll see:

1. ✅ Container status: "Up"
2. ✅ Log: "Streaming query started"
3. ✅ Spark UI: http://localhost:4040 accessible
4. ✅ Streaming tab shows active query
5. ✅ Input rate > 0 when data flows

---

**Wait 60 seconds after restart, then check http://localhost:4040** 🚀
