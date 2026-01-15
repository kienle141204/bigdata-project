#!/bin/bash
# Script khởi động nhanh Airflow trên EC2
# Usage: ./start-airflow.sh

set -e

echo "🚀 Starting Airflow for Premier League Batch Pipeline..."

# Kiểm tra .env file
if [ ! -f ../.env ]; then
    echo "❌ Error: .env file not found in parent directory!"
    echo "Please create .env with AWS credentials first."
    exit 1
fi

# Set Airflow UID
echo "Setting AIRFLOW_UID..."
echo "AIRFLOW_UID=$(id -u)" > .env

# Copy parent .env to current directory (for docker-compose to read)
cat ../.env >> .env

# Tạo thư mục nếu chưa có
mkdir -p dags logs plugins

# Build và khởi động
echo "📦 Building and starting Airflow containers..."
sudo docker-compose -f docker-compose.airflow.yml up -d --build

# Đợi khởi động
echo "⏳ Waiting for Airflow to initialize..."
sleep 30

# Kiểm tra trạng thái
echo "📊 Checking container status..."
sudo docker-compose -f docker-compose.airflow.yml ps

# Lấy IP
PUBLIC_IP=$(curl -s ifconfig.me 2>/dev/null || echo "<EC2-PUBLIC-IP>")

echo ""
echo "✅ Airflow started successfully!"
echo ""
echo "🌐 Airflow UI: http://${PUBLIC_IP}:8081"
echo "👤 Username: admin"
echo "🔑 Password: admin"
echo ""
echo "📋 Available DAGs:"
echo "   - premier_league_batch_scrape (Daily at 6:00 AM)"
echo "   - premier_league_backfill (Manual trigger)"
echo ""
echo "📝 To view logs: sudo docker-compose -f docker-compose.airflow.yml logs -f"
