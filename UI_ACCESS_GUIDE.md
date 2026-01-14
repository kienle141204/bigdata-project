# 📊 UI Access Guide

## Kafka UI & Spark UI - Quick Reference

---

## 🎯 Available UIs

### 1. **Kafka UI** ⭐
```
URL: http://localhost:8080
Status: Always available when Kafka is running
Service: kafka-ui
```

**Features:**
- 📊 View all Kafka topics
- 💬 Browse messages in topics  
- 👥 Monitor consumer groups & lag
- 📈 Broker health & metrics
- ⚡ Partition distribution
- 🔍 Search & filter messages

**Access:**
```bash
# Start services
docker-compose up -d

# Open browser
http://localhost:8080
```

---

### 2. **Spark UI** ⚡

#### Streaming Spark UI (NOW ALWAYS AVAILABLE! ⭐)
```
URL: http://localhost:4040
Status: Available 24/7 when streaming-etl is running
Service: streaming-etl
```

#### Batch Consumer Spark UI (Active when ETL runs)
```
URL: http://localhost:4041
Status: Available when kafka-consumer is processing
Service: kafka-consumer
```

**Features:**
- 📊 Job execution timeline
- 🔄 Stage progress & metrics
- 💾 Storage & memory usage
- 📈 DAG visualization
- ⚡ Executor metrics
- 🗂️ SQL queries (if any)

✅ **Update**: Spark Streaming UI giờ đã luôn sẵn sàng ngay khi container khởi động!

---

## 🚀 How to Access

### Quick Start:

```bash
# 1. Start all services
docker-compose up -d

# 2. Open Kafka UI (always available)
# Browser: http://localhost:8080

# 3. Trigger Spark to see Spark UI
# Option A: Let consumer auto-process
python scrape_to_s3.py --matchweek 1 --use-kafka

# Option B: Run manual ETL
python pipeline.py --seasons "2025/26"

# 4. Open Spark UI (while ETL is running)
# Browser: http://localhost:4040
```

---

## 📋 Port Summary

| Service | Port | URL | Status |
|---------|------|-----|--------|
| **Kafka UI** | 8080 | http://localhost:8080 | Always On |
| **Spark UI (Consumer)** | 4040 | http://localhost:4040 | When ETL runs |
| **Spark UI (Manual)** | 4041 | http://localhost:4041 | When manual job runs |
| Kafka Broker | 9092 | - | Internal |
| Zookeeper | 2181 | - | Internal |

---

## 🔍 What to Monitor

### Kafka UI - Key Metrics:

**Topics to check:**
```
- raw-match-data       → Incoming scraped data
- silver-processing    → ETL triggers
- dlq-failed-messages  → Failed messages
```

**Consumer Group:**
```
Group: premier_league_etl_group
Lag: Should be < 100 (ideally close to 0)
```

**Metrics:**
```
✅ Messages per second
✅ Consumer offset vs Log end offset
✅ Partition distribution
⚠️  Error rate (check DLQ topic)
```

---

### Spark UI - Key Metrics:

**When viewing Spark UI:**

**Jobs Tab:**
```
- Active jobs: Running stages
- Completed jobs: Success/failure status
- Duration: Processing time
```

**Stages Tab:**
```
- Task progress
- Input/Output size
- Shuffle read/write
```

**Storage Tab:**
```
- Cached RDDs/DataFrames
- Memory usage
```

**Executors Tab:**
```
- Active executors
- Memory & disk usage
- Task completion rate
```

---

## 🎯 Usage Scenarios

### Scenario 1: Monitor Kafka Messages

```bash
# 1. Start services
docker-compose up -d

# 2. Open Kafka UI
http://localhost:8080

# 3. Navigate to Topics > raw-match-data

# 4. Click "Messages" to view content
```

### Scenario 2: Watch ETL Processing

```bash
# Terminal 1: Start consumer
docker-compose up kafka-consumer

# Terminal 2: Send test data
python scrape_to_s3.py --matchweek 1 --use-kafka

# Browser: Watch in real-time
http://localhost:8080  # Kafka side
http://localhost:4040  # Spark side (when ETL triggers)
```

### Scenario 3: Debug Failed Messages

```bash
# 1. Check DLQ in Kafka UI
http://localhost:8080
Topics > dlq-failed-messages

# 2. View message details
# Click on each message to see error details
```

---

## 🆘 Troubleshooting

### Kafka UI not accessible?

```bash
# Check if service is running
docker-compose ps kafka-ui

# Restart
docker-compose restart kafka-ui

# Check logs
docker-compose logs kafka-ui
```

### Spark UI not showing?

```bash
# Spark UI only appears when job is RUNNING

# Check if Spark is processing
docker-compose logs -f kafka-consumer

# If no ETL running, trigger manually
python pipeline.py --seasons "2025/26"

# Then check: http://localhost:4040
```

### Port already in use?

```bash
# Check what's using the port
netstat -ano | findstr :8080
netstat -ano | findstr :4040

# Kill process or change port in docker-compose.yml
```

---

## 💡 Pro Tips

### Kafka UI Tips:

1. **Live Tail Messages**
   - Topics > raw-match-data > Messages > "Live Mode"
   - Watch messages arrive in real-time

2. **Consumer Lag Alert**
   - Consumers tab > Check lag
   - If lag > 100, consumer may be slow

3. **Topic Size**
   - Check retention & message count
   - Clean up if needed

### Spark UI Tips:

1. **DAG Visualization**
   - Jobs > Click job > "DAG Visualization"
   - See execution plan

2. **Slow Stage Detection**
   - Stages tab > Sort by duration
   - Identify bottlenecks

3. **Memory Issues**
   - Executors tab > Memory usage
   - If high, increase SPARK_DRIVER_MEMORY

---

## 📸 Screenshots Guide

### Kafka UI Main Page:
```
http://localhost:8080

You'll see:
- Cluster overview
- Topics list (3 topics)
- Brokers (1 broker)
- Consumer groups
```

### Kafka UI - Topic View:
```
http://localhost:8080/ui/clusters/premier-league-cluster/topics/raw-match-data

You'll see:
- Message count
- Partitions (3)
- Retention time
- Message browser
```

### Spark UI - Jobs:
```
http://localhost:4040

You'll see:
- Active jobs
- Completed jobs
- Job timeline
- Click job for details
```

---

## 🎓 Learning Resources

### Understanding Kafka UI:
- Topics = Where messages are stored
- Partitions = Parallel processing
- Consumer Groups = Who's reading
- Lag = How far behind consumers are

### Understanding Spark UI:
- Jobs = High-level operations
- Stages = Grouped tasks
- Tasks = Individual computations
- Executors = Worker processes

---

## ✅ Quick Check Commands

```bash
# Check if UIs are accessible
curl http://localhost:8080  # Kafka UI
curl http://localhost:4040  # Spark UI (if running)

# Check service status
docker-compose ps

# View logs
docker-compose logs -f kafka-ui
docker-compose logs -f kafka-consumer
```

---

## 🎉 Summary

**With your current setup:**

✅ **Kafka UI**: http://localhost:8080 (Always available)  
✅ **Spark UI**: http://localhost:4040 (When ETL runs)

**To start monitoring:**

```bash
# 1. Start services
docker-compose up -d

# 2. Access Kafka UI
http://localhost:8080

# 3. Trigger ETL to see Spark UI
python scrape_to_s3.py --matchweek 1 --use-kafka

# 4. Access Spark UI (during processing)
http://localhost:4040
```

---

**Happy Monitoring!** 📊
