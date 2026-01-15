# HƯỚNG DẪN TRIỂN KHAI CRAWLER LÊN EC2 (DOCKER HUB: kienle1412)

## 0. Build & Push Image (Tại máy cá nhân - Local)
docker login
docker build -t kienle1412/bigdata:latest .
docker push kienle1412/bigdata:latest

## 1. Khởi tạo trên EC2
# Tải image từ Docker Hub
docker pull kienle1412/bigdata:latest

# Khởi tạo manifest (Chỉ chạy 1 lần duy nhất)
docker run --rm --env-file .env kienle1412/bigdata:latest python scripts/init_s3_status.py

## 2. Luồng 2: Streaming (Chạy liên tục - Real-time)
# Tự động quét 38 vòng đấu mỗi 10 phút, gửi dữ liệu mới vào Kafka.
docker run -d --name pl-stream --restart always --env-file .env kienle1412/bigdata:latest \
  python kafka/producer.py --continuous --interval 600 --season "2025/26"

## 3. Luồng 1: Batch (Chạy 2 lần/ngày)
### Cách A: Dùng Docker (Chạy liên tục với chu kỳ 12 giờ)
# Tự động cào và xử lý Bronze -> Silver -> Gold 2 lần/ngày
docker run -d --name pl-batch --restart always --env-file .env kienle1412/bigdata:latest \
  python scrape_to_s3.py --continuous --interval 43200 --season "2025/26" --workers 1 --run-etl

### Cách B: Dùng Crontab (Khuyên dùng - Ổn định nhất)
# Thêm dòng sau để chạy vào 0giờ và 12giờ mỗi ngày:
0 0,12 * * * docker run --rm --env-file /home/ec2-user/.env kienle1412/bigdata:latest python scrape_to_s3.py --season "2025/26" --workers 3 --run-etl

## 4. Quản lý và Kiểm tra
docker logs -f pl-stream
docker logs -f pl-batch
docker ps

## 5. Theo dõi qua Kafka UI (Web Interface)
# Chạy container Kafka UI để xem dữ liệu real-time
docker run -d --name kafka-ui -p 8080:8080 \
  -e KAFKA_CLUSTERS_0_NAME=production \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092 \
  provectuslabs/kafka-ui:latest

# LƯU Ý: Phải mở port 8080 trong AWS Security Group để truy cập từ trình duyệt:
# http://<EC2_PUBLIC_IP>:8080
