# 🚀 Quick Start: Streaming + UI Access

## TL;DR - Chạy Ngay

```bash
# 1. Khởi động tất cả
docker-compose up -d zookeeper kafka kafka-ui streaming-etl

# 2. Đợi 30 giây
sleep 30

# 3. Test UI
python test_ui_access.py

# 4. Mở browser
# - Kafka UI:  http://localhost:8080
# - Spark UI:  http://localhost:4040

# 5. Gửi dữ liệu test
docker-compose up scraper
```

---

## 📊 UI Endpoints

| UI | URL | Trạng thái |
|----|-----|-----------|
| **Kafka UI** | http://localhost:8080 | ✅ Always ON |
| **Spark Streaming UI** | http://localhost:4040 | ✅ Always ON (sau update) |
| **Spark Batch UI** | http://localhost:4041 | ⏸️ Khi batch ETL chạy |

---

## 🔧 Commands Chính

### Start Streaming
```bash
docker-compose up -d streaming-etl
```

### Start Batch (profile)
```bash
docker-compose --profile batch up -d kafka-consumer
```

### Run Both
```bash
docker-compose --profile batch up -d streaming-etl kafka-consumer
```

### View Logs
```bash
# Streaming
docker-compose logs -f streaming-etl

# Kafka
docker-compose logs -f kafka

# All
docker-compose logs -f
```

### Stop
```bash
# Just streaming
docker-compose stop streaming-etl

# Everything
docker-compose down
```

---

## 📖 Chi Tiết Docs

- **[STREAMING_UI_GUIDE.md](STREAMING_UI_GUIDE.md)** - Hướng dẫn đầy đủ nhất ⭐
- **[DUAL_MODE_GUIDE.md](DUAL_MODE_GUIDE.md)** - So sánh Batch vs Streaming
- **[UI_ACCESS_GUIDE.md](UI_ACCESS_GUIDE.md)** - UI access reference
- **[SPARK_UI_GUIDE.md](SPARK_UI_GUIDE.md)** - Spark UI chi tiết

---

## ✅ Checklist

- [ ] Docker đang chạy
- [ ] `.env` file đã config AWS credentials
- [ ] Port 8080 và 4040 không bị chiếm
- [ ] Đã chạy `docker-compose up -d`
- [ ] Đã đợi 30 giây
- [ ] Test: `python test_ui_access.py` ✅
- [ ] Browser mở được http://localhost:8080 ✅
- [ ] Browser mở được http://localhost:4040 ✅

---

## 🆘 Troubleshooting

```bash
# UI không truy cập được?
docker-compose restart kafka-ui streaming-etl
sleep 30
python test_ui_access.py

# Container bị lỗi?
docker-compose logs streaming-etl

# Port bị chiếm?
netstat -ano | findstr :4040

# Clean reset
docker-compose down -v
docker-compose up -d
```

---

**Đọc [STREAMING_UI_GUIDE.md](STREAMING_UI_GUIDE.md) để biết chi tiết!** 📚
