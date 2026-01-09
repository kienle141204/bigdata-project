# Hướng Dẫn Chạy Dự Án Bằng Docker

Dự án này sử dụng Docker để đơn giản hóa việc chạy ứng dụng. Bạn không cần cài đặt Python, Java, Chrome hay Spark trên máy tính cá nhân.

Hệ thống được chia thành 2 service chính:
1.  **scraper**: Dùng để cào dữ liệu (có sẵn Chrome, Python, Selenium).
2.  **spark-job**: Dùng để xử lý dữ liệu (có sẵn Java 17, Spark, Python).

---

## 1. Yêu Cầu
*   Đã cài đặt [Docker Desktop](https://www.docker.com/products/docker-desktop/) trên máy tính.
*   (Khuyên dùng) Cấu hình RAM cho Docker/WSL ít nhất **4GB-8GB** để chạy mượt mà (xem phần Xử Lý Lỗi cuối file).

## 2. Thiết Lập Ban Đầu (Build)
Trước khi chạy lần đầu tiên:

```bash
docker-compose build
```

---

## 3. Cách Chạy Các Lệnh

### A. Cào Dữ Liệu (Scraping)
Sử dụng service `scraper`.

**Cào nhiều vòng đấu song song (Multithreading within Container):**
Đây là cách nhanh nhất và tối ưu nhất.

```bash
# Cào vòng 1, 2 và 3 cùng lúc với 3 luồng (3 trình duyệt)
docker-compose run --rm scraper python scrape_to_s3.py --matchweek 1 2 3 --workers 3
```

**Các tham số quan trọng:**
*   `--matchweek 1 2 3 ...`: Danh sách các vòng đấu muốn cào.
*   `--workers N`: Số lượng luồng chạy song song (số trình duyệt bật cùng lúc). Nên để **≤ 5** để tránh treo máy.
*   `--season 2024/25`: Mùa giải (mặc định 2025/26).

### B. Chạy Pipeline Xử Lý Dữ Liệu (Spark ETL)
Sử dụng service `spark-job`. Service này đã có sẵn Java 17 để chạy PySpark.

```bash
# Xử lý dữ liệu cho vòng 1, 2, 3 (Bỏ qua bước cào vì đã làm ở bước trên)
docker-compose run --rm spark-job python pipeline.py --matchweeks 1 2 3 --skip-scrape
```

### C. Chạy Toàn Bộ (Cào + Xử Lý)
Nếu bạn muốn chạy tuần tự (Cào xong -> Xử lý luôn) trong cùng 1 môi trường (lưu ý: môi trường `spark-job` này không có Chrome nên chỉ chạy được nếu code scraping không dùng Selenium hoặc bạn đã tùy chỉnh image):
*Hiện tại kiến trúc tối ưu là chạy Bước A trước, xong chạy Bước B.*

---

## 4. Mẹo Vặt & Xử Lý Lỗi

**Lỗi: "Chromium crashed" / "no such execution context"**
*   **Nguyên nhân:** Máy thiếu RAM khi chạy nhiều trình duyệt Chrome cùng lúc.
*   **Khắc phục:**
    1.  Giảm số lượng `--workers` xuống (ví dụ còn 2 hoặc 3).
    2.  Tăng giới hạn RAM cho WSL2 (xem file `.wslconfig` trong `C:\Users\<User>\`).

**Lỗi: "Java Heap Space" (PySpark)**
*   Service `spark-job` đã được cấu hình sẵn 4GB RAM cho Driver. Nếu vẫn lỗi, hãy tăng thêm trong file `docker-compose.yml`.

**Lệnh `docker-compose run --rm` nghĩa là gì?**
*   `run`: Chạy một container mới từ image.
*   `--rm`: Tự động xóa container sau khi chạy xong để dọn rác, tiết kiệm ổ cứng.
