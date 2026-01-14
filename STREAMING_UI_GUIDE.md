# 🎯 Hướng Dẫn Chạy Streaming và Truy Cập UI 24/7

## 📊 Tổng Quan

Hệ thống có **2 UI chính** để monitor:
1. **Kafka UI** - Monitor Kafka topics, messages, consumers (luôn sẵn sàng 24/7)
2. **Spark UI** - Monitor Spark Streaming processing (giờ đã sẵn sàng 24/7 sau cập nhật!)

---

## 🚀 Cách Chạy Luồng Streaming

### Bước 1: Khởi động Infrastructure

```bash
# Khởi động Zookeeper và Kafka
docker-compose up -d zookeeper kafka kafka-ui

# Đợi Kafka khởi động hoàn toàn (10-15 giây)
docker-compose logs -f kafka | grep "started"
```

### Bước 2: Khởi động Spark Streaming ETL

```bash
# Khởi động streaming service
docker-compose up -d streaming-etl

# Xem logs để confirm
docker-compose logs -f streaming-etl
```

**Chờ thấy message:**
```
✅ Spark session created
🎯 Initializing Spark UI...
✅ Spark UI initialized and accessible at: http://localhost:4040
📡 Connecting to Kafka stream...
✅ Connected to Kafka stream
✅ Streaming query started!
```

### Bước 3: Kiểm tra UI đã sẵn sàng

```bash
# Kafka UI
curl http://localhost:8080
# Nếu thành công → mở browser: http://localhost:8080

# Spark UI  
curl http://localhost:4040
# Nếu thành công → mở browser: http://localhost:4040
```

### Bước 4: (Optional) Gửi dữ liệu test

```bash
# Chạy scraper để gửi data vào Kafka
docker-compose up scraper

# Hoặc chỉ chạy 1 matchweek
docker-compose run --rm scraper python scrape_to_s3.py --matchweek 1 --use-kafka
```

---

## 🌐 Truy Cập UI 24/7

### 1. Kafka UI (http://localhost:8080) ⭐

**Luôn sẵn sàng** khi Kafka đang chạy.

**Những gì bạn có thể thấy:**

#### Topics Tab
- `raw-match-data` - Messages từ scraper
- `silver-processing` - Batch ETL triggers
- `dlq-failed-messages` - Failed messages

#### Messages Browser
- Click vào topic → Messages
- Xem nội dung JSON của mỗi message
- Filter theo time range
- Enable "Live Mode" để xem real-time

#### Consumers Tab
- Consumer Group: `premier_league_streaming`
- Lag: Số messages chưa xử lý
- Offset: Vị trí đọc hiện tại

#### Brokers Tab
- Broker health
- Partition distribution
- Disk usage

---

### 2. Spark Streaming UI (http://localhost:4040) ⚡

**Giờ đã luôn sẵn sàng 24/7!** (Sau khi cập nhật code)

**Cách hoạt động mới:**
- ✅ UI xuất hiện ngay khi container start
- ✅ Không cần đợi data từ Kafka
- ✅ Có thể truy cập bất cứ lúc nào

**Những gì bạn có thể thấy:**

#### Streaming Tab (Main)
```
Active Streaming Queries:
- Query 1: raw-match-data → silver/streaming/
  • Input Rate: X records/sec
  • Process Rate: Y records/sec  
  • Batch Duration: Average time per micro-batch
  • Completed Batches: List of processed batches
```

#### Jobs Tab
- Tất cả Spark jobs (streaming micro-batches)
- Duration của mỗi job
- Success/failure status

#### Stages Tab
- Chi tiết các stages trong job
- Task progress
- Input/Output metrics
- Shuffle read/write

#### Storage Tab
- Cached DataFrames
- Memory usage
- Persist RDDs

#### Environment Tab
- Spark configuration
- System properties
- Runtime information

#### Executors Tab
- Active executors
- Memory & disk usage
- Task metrics
- GC time

#### SQL Tab (nếu có SQL queries)
- DataFrame operations
- Query plans
- Execution details

---

## 🎯 Commands Summary

### Khởi động toàn bộ hệ thống streaming
```bash
# All-in-one command
docker-compose up -d zookeeper kafka kafka-ui streaming-etl

# Đợi 30 giây, sau đó mở browser:
# - http://localhost:8080 (Kafka UI)
# - http://localhost:4040 (Spark UI)
```

### Kiểm tra trạng thái
```bash
# Xem tất cả services
docker-compose ps

# Xem logs streaming
docker-compose logs -f streaming-etl

# Xem logs Kafka
docker-compose logs -f kafka
```

### Gửi dữ liệu test
```bash
# Run scraper
docker-compose up scraper

# Hoặc manual test message (Python)
python -c "
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_data = {
    'match_id': 999999,
    'season': '2025-26',
    'matchweek': 99,
    'match_info': {
        'home_team': 'Test Home',
        'away_team': 'Test Away',
        'home_score': 2,
        'away_score': 1
    }
}

producer.send('raw-match-data', value=test_data)
producer.flush()
print('✅ Test message sent!')
"
```

### Dừng hệ thống
```bash
# Dừng streaming nhưng giữ Kafka
docker-compose stop streaming-etl

# Dừng toàn bộ
docker-compose down

# Dừng và xóa volumes (clean slate)
docker-compose down -v
```

---

## 📊 Monitoring Workflow

### Workflow thông thường:

```
1. Start services
   ↓
2. Check Kafka UI (http://localhost:8080)
   → Verify Kafka is healthy
   → Check topics exist
   ↓
3. Check Spark UI (http://localhost:4040)  
   → Verify Spark session active
   → See "Streaming" tab
   ↓
4. Send data via scraper
   ↓
5. Monitor in real-time:
   → Kafka UI: Watch messages arrive in raw-match-data
   → Spark UI: Watch streaming batches process
   ↓
6. Verify output:
   → Check S3 bucket (silver/streaming/)
   → See Parquet files with processed data
```

---

## 🔍 Troubleshooting

### Kafka UI không truy cập được?

```bash
# Check service
docker-compose ps kafka-ui
# Status phải là "Up"

# Check logs
docker-compose logs kafka-ui

# Restart
docker-compose restart kafka-ui

# Wait 10 seconds
sleep 10

# Try again
curl http://localhost:8080
```

### Spark UI không truy cập được?

**Sau khi cập nhật code, UI nên xuất hiện ngay!**

```bash
# Check container
docker-compose ps streaming-etl
# Status phải là "Up"

# Check logs  
docker-compose logs --tail=50 streaming-etl

# Phải thấy:
# ✅ Spark UI initialized and accessible at: http://localhost:4040

# Nếu không thấy, restart
docker-compose restart streaming-etl

# Wait for initialization
sleep 30

# Try accessing
curl http://localhost:4040
```

### Port bị chiếm?

```bash
# Check port 8080 (Kafka UI)
netstat -ano | findstr :8080

# Check port 4040 (Spark UI)
netstat -ano | findstr :4040

# Nếu bị chiếm, kill process hoặc đổi port trong docker-compose.yml
```

### Streaming không xử lý data?

```bash
# 1. Check Kafka có data không?
# Kafka UI → Topics → raw-match-data
# Xem message count > 0

# 2. Check Spark logs
docker-compose logs -f streaming-etl | grep "Batch"
# Phải thấy: "Batch X completed"

# 3. Check S3 output
# Verify files được tạo trong s3://bucket/premier_league/silver/streaming/

# 4. Check consumer lag
# Kafka UI → Consumers → premier_league_streaming
# Lag phải giảm dần về 0
```

---

## 💡 Pro Tips

### Kafka UI - Advanced

**1. Live Message Monitoring**
```
Topics → raw-match-data → Messages
→ Enable "Live Mode" (top right)
→ Watch messages stream in real-time
```

**2. Consumer Lag Monitoring**
```
Consumers → premier_league_streaming
→ Check "Lag" column
→ If lag > 100: Streaming may be slow
→ If lag growing: Problem detected!
```

**3. Topic Configuration**
```
Topics → raw-match-data → Settings
→ Check retention (168 hours = 7 days)
→ Check partitions (should be 3)
→ Check replication (should be 1)
```

### Spark UI - Advanced

**1. Streaming Query Details**
```
Streaming Tab → Click query name
→ See detailed metrics
→ Input/Output rates
→ Batch durations over time
```

**2. Identify Slow Batches**
```
Streaming Tab → Completed Batches
→ Sort by duration
→ Click batch ID for details
→ See which operation is slow
```

**3. Check Memory Usage**
```
Executors Tab → Storage Memory
→ If > 80%: Increase SPARK_DRIVER_MEMORY
→ Current: 4g (in docker-compose.yml)
```

---

## ✅ Best Practices

### 1. Luôn kiểm tra Kafka UI trước
- Verify topics tồn tại
- Check broker health
- Confirm messages đang được produced

### 2. Monitor Spark UI thường xuyên
- Check streaming rate
- Verify no failed batches
- Monitor memory usage

### 3. Watch consumer lag
- Lag = 0: Perfect!
- Lag < 100: Acceptable
- Lag > 1000: Need optimization
- Lag growing: PROBLEM!

### 4. Keep logs open khi develop
```bash
# Terminal 1: Streaming logs
docker-compose logs -f streaming-etl

# Terminal 2: Kafka logs  
docker-compose logs -f kafka

# Browser: UIs
# - http://localhost:8080
# - http://localhost:4040
```

---

## 🎉 Quick Start Checklist

- [ ] `docker-compose up -d zookeeper kafka kafka-ui streaming-etl`
- [ ] Đợi 30 giây
- [ ] Mở http://localhost:8080 → Thấy Kafka UI ✅
- [ ] Mở http://localhost:4040 → Thấy Spark UI ✅
- [ ] `docker-compose logs -f streaming-etl` → Thấy "✅ Streaming query started!" ✅
- [ ] Chạy scraper: `docker-compose up scraper` ✅
- [ ] Kafka UI → Topics → raw-match-data → Thấy messages ✅
- [ ] Spark UI → Streaming tab → Thấy batches processing ✅
- [ ] Verify S3 output → Thấy Parquet files ✅

**Nếu tất cả ✅ → Hệ thống streaming hoạt động hoàn hảo!** 🚀

---

## 📚 Additional Resources

### Files liên quan:
- `streaming_etl.py` - Main streaming code
- `docker-compose.yml` - Service configurations  
- `DUAL_MODE_GUIDE.md` - Batch vs Streaming comparison
- `SPARK_UI_GUIDE.md` - Old UI guide (có thể outdated)

### Các commands hữu ích khác:
```bash
# Xem tất cả containers
docker ps

# Xem resource usage
docker stats

# Clean up everything
docker-compose down -v
docker system prune -a

# Rebuild sau khi sửa code
docker-compose build streaming-etl
docker-compose up -d streaming-etl
```

---

**Happy Streaming and Monitoring! 📊🚀**
