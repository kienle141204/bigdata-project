# 🏭 BATCH PIPELINE - Hướng dẫn triển khai

Pipeline cào dữ liệu theo lịch (scheduled scraping) và xử lý ETL batch.

## 📋 Mô tả

Luồng này bao gồm:
1. **Scraper**: Cào dữ liệu từ Premier League website
2. **ETL Batch**: Bronze → Silver → Gold transformation
3. **Lưu trữ**: Upload lên AWS S3

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

# Selenium
HEADLESS=true
REQUEST_DELAY=2
EOF
```

### Bước 3: Chạy Batch Pipeline

#### Option A: Chạy một lần (One-shot)

```bash
# Cào 1 matchweek cụ thể
sudo docker run --rm \
  --name pl-batch \
  --env-file .env \
  --shm-size=2g \
  kienle1412/bigdata:latest \
  python scrape_to_s3.py --matchweek 1 --season "2025/26" --workers 1

# Cào nhiều matchweeks
sudo docker run --rm \
  --name pl-batch \
  --env-file .env \
  --shm-size=2g \
  kienle1412/bigdata:latest \
  python scrape_to_s3.py --matchweek 1 2 3 4 5 --season "2025/26" --workers 3

# Cào toàn bộ mùa giải + chạy ETL
sudo docker run --rm \
  --name pl-batch \
  --env-file .env \
  --shm-size=2g \
  kienle1412/bigdata:latest \
  python scrape_to_s3.py --matchweek 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 --season "2024/25" --workers 5 --run-etl
```

#### Option B: Chạy liên tục (Continuous/Scheduled)

```bash
# Chạy nền, kiểm tra mỗi 1 giờ
sudo docker run -d \
  --name pl-batch \
  --env-file .env \
  --shm-size=2g \
  --restart unless-stopped \
  kienle1412/bigdata:latest \
  python scrape_to_s3.py --continuous --interval 3600 --season "2025/26" --workers 1 --run-etl
```

### Bước 4: Kiểm tra logs

```bash
# Xem logs realtime
sudo docker logs -f pl-batch

# Xem 100 dòng cuối
sudo docker logs --tail 100 pl-batch
```

## 📊 Các tham số quan trọng

| Tham số | Mô tả | Mặc định |
|---------|-------|----------|
| `--matchweek` | Danh sách matchweek cần cào | Bắt buộc |
| `--season` | Mùa giải (ví dụ: "2025/26") | "2025/26" |
| `--workers` | Số thread song song | 1 |
| `--delay` | Delay giữa các match (giây) | 2.0 |
| `--continuous` | Chạy liên tục theo lịch | False |
| `--interval` | Khoảng cách giữa các chu kỳ (giây) | 43200 (12h) |
| `--run-etl` | Tự động chạy ETL sau khi cào | False |

## 🗓️ Các mùa giải hỗ trợ

- 2025/26, 2024/25, 2023/24, 2022/23, 2021/22
- 2020/21, 2019/20, 2018/19, 2017/18, 2016/17
- 2015/16, 2014/15, 2013/14, 2012/13, 2011/12, 2010/11

## ⚠️ Lưu ý

1. **shm-size**: Bắt buộc phải có `--shm-size=2g` để Chrome hoạt động ổn định
2. **Workers**: Không nên dùng quá 5 workers trên EC2 t2.medium
3. **Memory**: Mỗi worker cần ~500MB RAM cho Chrome

## 🔍 Troubleshooting

```bash
# Xem trạng thái container
sudo docker ps -a

# Vào container để debug
sudo docker exec -it pl-batch bash

# Kiểm tra S3 upload
aws s3 ls s3://your-bucket/premier_league/bronze/ --recursive
```
