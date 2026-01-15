# 🌊 STREAMING PIPELINE - Hướng dẫn triển khai

Pipeline xử lý dữ liệu real-time với Kafka và Spark Structured Streaming.

## 📋 Mô tả

Luồng này bao gồm:
1. **Kafka**: Message broker cho data streaming
2. **Producer**: Gửi dữ liệu vào Kafka topic
3. **Spark Streaming**: Consume từ Kafka, xử lý và ghi vào Iceberg tables trên S3
4. **Kafka UI**: Giao diện quản lý Kafka

## 🚀 Triển khai trên EC2

### Bước 1: Dọn dẹp containers cũ (nếu có)

```bash
# Dừng tất cả containers
sudo docker stop $(sudo docker ps -aq)

# Xóa tất cả containers
sudo docker rm $(sudo docker ps -aq)

# (Tùy chọn) Xóa images không dùng
sudo docker image prune -f
```

### Bước 2: Tạo file môi trường

```bash
cat > .env << 'EOF'
# AWS S3 Configuration
AWS_S3_BUCKET=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=ap-southeast-1
AWS_S3_PREFIX=premier_league

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC_RAW=premier_league_raw
EOF
```

### Bước 3: Tạo Docker Network

```bash
sudo docker network create pl-network
```

### Bước 4: Khởi động Infrastructure (Zookeeper + Kafka)

```bash
# 1. Zookeeper
sudo docker run -d \
  --name zookeeper \
  --network pl-network \
  --restart unless-stopped \
  -e ALLOW_ANONYMOUS_LOGIN=yes \
  public.ecr.aws/bitnami/zookeeper:3.9

# 2. Kafka
sudo docker run -d \
  --name kafka \
  --network pl-network \
  --restart unless-stopped \
  -p 29092:29092 \
  -e ALLOW_ANONYMOUS_LOGIN=yes \
  -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:29092 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true \
  -e KAFKA_CFG_MESSAGE_MAX_BYTES=10485760 \
  public.ecr.aws/bitnami/kafka:3.7

# Đợi Kafka khởi động
sleep 10
```

### Bước 5: Khởi động Kafka UI (Tùy chọn)

```bash
sudo docker run -d \
  --name kafka-ui \
  --network pl-network \
  --restart unless-stopped \
  -p 8080:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092 \
  -e KAFKA_CLUSTERS_0_ZOOKEEPER=zookeeper:2181 \
  provectuslabs/kafka-ui:latest
```

**Truy cập:** http://<EC2-PUBLIC-IP>:8080

### Bước 6: Khởi động Producer

```bash
# Producer sẽ đọc từ S3 Bronze và gửi vào Kafka
sudo docker run -d \
  --name producer \
  --network pl-network \
  --env-file .env \
  --restart unless-stopped \
  kienle1412/bigdata:latest \
  python kafka/producer.py
```

### Bước 7: Khởi động Spark Streaming Consumer

```bash
sudo docker run -d \
  --name spark-streaming \
  --network pl-network \
  --env-file .env \
  --memory=2g \
  --memory-reservation=1g \
  -p 4040:4040 \
  --restart unless-stopped \
  kienle1412/bigdata:latest \
  python kafka/spark_consumer.py
```

**Truy cập Spark UI:** http://<EC2-PUBLIC-IP>:4040

## 📊 Kiểm tra trạng thái

```bash
# Xem tất cả containers
sudo docker ps

# Kết quả mong đợi:
# NAMES           STATUS
# zookeeper       Up
# kafka           Up
# kafka-ui        Up
# producer        Up
# spark-streaming Up
```

## 📝 Xem Logs

```bash
# Logs Kafka
sudo docker logs -f kafka

# Logs Producer
sudo docker logs -f producer

# Logs Spark Streaming (quan trọng nhất)
sudo docker logs -f spark-streaming

# Logs với timestamp
sudo docker logs -f --timestamps spark-streaming
```

## 🔧 Script khởi động nhanh (All-in-one)

Tạo file `start-streaming.sh`:

```bash
cat > start-streaming.sh << 'SCRIPT'
#!/bin/bash
set -e

echo "🚀 Starting Streaming Pipeline..."

# Create network
sudo docker network create pl-network 2>/dev/null || true

# Start Zookeeper
echo "Starting Zookeeper..."
sudo docker run -d --name zookeeper --network pl-network \
  -e ALLOW_ANONYMOUS_LOGIN=yes \
  --restart unless-stopped \
  public.ecr.aws/bitnami/zookeeper:3.9

sleep 5

# Start Kafka
echo "Starting Kafka..."
sudo docker run -d --name kafka --network pl-network \
  -p 29092:29092 \
  -e ALLOW_ANONYMOUS_LOGIN=yes \
  -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:29092 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true \
  --restart unless-stopped \
  public.ecr.aws/bitnami/kafka:3.7

sleep 10

# Start Kafka UI
echo "Starting Kafka UI..."
sudo docker run -d --name kafka-ui --network pl-network \
  -p 8080:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092 \
  --restart unless-stopped \
  provectuslabs/kafka-ui:latest

# Start Producer
echo "Starting Producer..."
sudo docker run -d --name producer --network pl-network \
  --env-file .env \
  --restart unless-stopped \
  kienle1412/bigdata:latest \
  python kafka/producer.py

# Start Spark Streaming
echo "Starting Spark Streaming..."
sudo docker run -d --name spark-streaming --network pl-network \
  --env-file .env \
  --memory=2g \
  -p 4040:4040 \
  --restart unless-stopped \
  kienle1412/bigdata:latest \
  python kafka/spark_consumer.py

echo "✅ All services started!"
echo "📊 Kafka UI: http://$(curl -s ifconfig.me):8080"
echo "⚡ Spark UI: http://$(curl -s ifconfig.me):4040"
SCRIPT

chmod +x start-streaming.sh
```

Chạy: `./start-streaming.sh`

## 🛑 Script dừng (All-in-one)

```bash
cat > stop-streaming.sh << 'SCRIPT'
#!/bin/bash
echo "🛑 Stopping all streaming containers..."
sudo docker stop spark-streaming producer kafka-ui kafka zookeeper 2>/dev/null
sudo docker rm spark-streaming producer kafka-ui kafka zookeeper 2>/dev/null
echo "✅ All containers stopped and removed"
SCRIPT

chmod +x stop-streaming.sh
```

## ⚠️ Lưu ý quan trọng

1. **Memory**: Spark cần ít nhất 2GB RAM, nên EC2 cần tối thiểu t2.medium (4GB)
2. **Security Group**: Mở ports 8080 (Kafka UI) và 4040 (Spark UI)
3. **Thứ tự khởi động**: Zookeeper → Kafka → Producer → Spark Streaming
4. **S3 Access**: Đảm bảo .env có đúng AWS credentials

## 🔍 Troubleshooting

### Lỗi "bucket is null/empty"
```bash
# Kiểm tra .env file
cat .env

# Đảm bảo AWS_S3_BUCKET có giá trị
```

### Lỗi Kafka connection
```bash
# Kiểm tra Kafka đang chạy
sudo docker logs kafka

# Test kết nối
sudo docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:29092
```

### Spark không nhận data
```bash
# Kiểm tra Kafka topic có data
sudo docker exec -it kafka kafka-console-consumer.sh \
  --topic premier_league_raw \
  --from-beginning \
  --bootstrap-server localhost:29092 \
  --max-messages 5
```
