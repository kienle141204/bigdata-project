# Hướng Dẫn Chạy Dự Án Bằng Docker

Dự án này sử dụng Docker để đơn giản hóa việc chạy ứng dụng. Bạn không cần cài đặt Python, Java, Chrome hay Spark trên máy tính cá nhân.

Hệ thống hỗ trợ 2 luồng xử lý chính:
1.  **Luồng Batch (Bình thường)**: Xử lý dữ liệu theo mùa/vòng đấu và lưu vào CSV.
2.  **Luồng Streaming (Kafka + Iceberg)**: Xử lý dữ liệu thời gian thực qua Kafka và lưu vào Apache Iceberg.

---

## 1. Yêu Cầu
*   Đã cài đặt [Docker Desktop](https://www.docker.com/products/docker-desktop/) trên máy tính.
*   (Khuyên dùng) Cấu hình RAM cho Docker/WSL ít nhất **4GB-8GB** (Spark và Kafka tốn khá nhiều RAM).

## 2. Thiết Lập Ban Đầu (Build)
Trước khi chạy lần đầu tiên:
```bash
docker-compose build
```

---

## 3. Luồng 1: Batch Processing (Normal Flow)
Luồng này sử dụng kiến trúc Medallion: **Bronze (JSON) -> Silver (CSV Master) -> Gold (CSV Team)**.

### Bước A: Cào Dữ Liệu (Bronze)
```bash
# Cào vòng 1 mùa 2011/12 và lưu JSON vào S3 Bronze
docker-compose run --rm scraper python scrape_to_s3.py --matchweek 1 --season "2011/12" --workers 1
```

### Bước B: Xử Lý Silver & Gold
Bạn có thể chạy toàn bộ pipeline hoặc chỉ tạo lại layer Gold.
```bash
# 1. Chạy toàn bộ: Bronze -> Silver CSV -> Gold CSV
docker-compose run --rm spark-job python pipeline.py --seasons "2011/12" --create-gold

# 2. Chỉ tạo lại các file Gold (nếu Silver đã có sẵn dữ liệu)
docker-compose run --rm spark-job python pipeline.py --gold-only
```
*Kết quả lưu tại:* `s3://.../premier_league/gold/{Team_Name}/`

---

## 4. Luồng 2: Streaming Processing (Kafka + Iceberg)
Luồng này sử dụng kiến trúc: **Kafka (Bronze) -> Spark Streaming (Silver Iceberg) -> CSV (Gold)**.

### Bước A: Khởi Chạy Hệ Thống Streaming
```bash
# Chạy các dịch vụ nền (Kafka, Zookeeper, Spark Streaming)
docker-compose up -d zookeeper kafka kafka-ui spark-streaming
```
*Bạn có thể xem UI Spark tại: `http://localhost:4040` và Kafka UI tại `http://localhost:8080`.*

### Bước B: Gửi Dữ Liệu Vào Kafka (Producer)
```bash
# Cào dữ liệu và đẩy vào hàng đợi Kafka
docker-compose run --rm scraper python kafka/producer.py --matchweek 1 --season "2011/12"
```

### Bước C: Đồng Bộ Sang Gold Layer (Sync to CSV)
Sau khi Spark Streaming đã ghi dữ liệu vào bảng Iceberg, chạy lệnh này để xuất ra định dạng CSV cho khách hàng:
```bash
docker-compose run --rm spark-job python kafka/silver_to_gold_iceberg.py
```
*Kết quả lưu tại:* `s3://.../premier_league/gold_iceberg/{Team_Name}/`

---

## 5. Mẹo Vặt & Xử Lý Lỗi

**Lỗi: "Chromium crashed"**
*   **Nguyên nhân:** Thiếu RAM.
*   **Khắc phục:** Giảm `--workers` xuống (ví dụ còn 1 hoặc 2) hoặc tăng RAM cho Docker.

**Kiểm tra Logs của Streaming:**
```bash
docker-compose logs -f spark-streaming
```

**Lệnh `docker-compose run --rm`:**
*   `run`: Chạy một container mới.
*   `--rm`: Tự động xóa container sau khi chạy xong để tiết kiệm ổ cứng.
