# PowerShell script để khởi động Airflow trên Windows
# Usage: .\start-airflow.ps1

$ErrorActionPreference = "Stop"

Write-Host "[START] Starting Airflow for Premier League Batch Pipeline..." -ForegroundColor Green

# Kiểm tra .env file
if (-not (Test-Path "..\.env")) {
    Write-Host "[ERROR] .env file not found in parent directory!" -ForegroundColor Red
    Write-Host "Please create .env with AWS credentials first."
    exit 1
}

# Set Airflow UID (Windows không có id -u, dùng giá trị mặc định)
Write-Host "[SETUP] Setting AIRFLOW_UID..." -ForegroundColor Yellow
$AIRFLOW_UID = 1000  # Giá trị mặc định cho Windows
"AIRFLOW_UID=$AIRFLOW_UID" | Out-File -FilePath ".env" -Encoding utf8

# Copy parent .env to current directory
Write-Host "[SETUP] Copying environment variables..." -ForegroundColor Yellow
Get-Content "..\.env" | Add-Content ".env"

# Tạo thư mục nếu chưa có
Write-Host "[SETUP] Creating directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "dags" | Out-Null
New-Item -ItemType Directory -Force -Path "logs" | Out-Null
New-Item -ItemType Directory -Force -Path "plugins" | Out-Null

# Build và khởi động
Write-Host "[BUILD] Building and starting Airflow containers..." -ForegroundColor Green
docker-compose -f docker-compose.airflow.yml up -d --build

# Đợi khởi động
Write-Host "[WAIT] Waiting for Airflow to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Kiểm tra trạng thái
Write-Host "[STATUS] Checking container status..." -ForegroundColor Green
docker-compose -f docker-compose.airflow.yml ps

# Import Airflow Variables tự động
Write-Host ""
Write-Host "[VARIABLES] Importing Airflow Variables..." -ForegroundColor Cyan
if (Test-Path "airflow-variables.json") {
    Write-Host "[INFO] Found airflow-variables.json, waiting for Airflow to be ready..." -ForegroundColor Yellow
    
    # Đợi thêm một chút để Airflow sẵn sàng
    Start-Sleep -Seconds 10
    
    # Chạy import script trong container (file đã được mount)
    $importResult = docker exec airflow python /opt/airflow/auto-import-variables.py /tmp/airflow-variables.json 2>&1
    
    if ($importResult -match "SUCCESS") {
        Write-Host "[OK] Variables imported successfully!" -ForegroundColor Green
        $importResult | Select-String -Pattern "OK:|ERROR:" | ForEach-Object {
            if ($_ -match "OK:") {
                Write-Host "  $_" -ForegroundColor Green
            } else {
                Write-Host "  $_" -ForegroundColor Yellow
            }
        }
    } elseif ($importResult -match "INFO:") {
        Write-Host "[INFO] Variables already imported or Airflow not ready yet." -ForegroundColor Yellow
        Write-Host "[TIP] Run manually if needed: .\import-variables.ps1" -ForegroundColor Gray
    } else {
        Write-Host "[WARNING] Variable import had issues. Check manually:" -ForegroundColor Yellow
        Write-Host "  .\import-variables.ps1" -ForegroundColor Gray
        Write-Host "[DEBUG] Import output:" -ForegroundColor Gray
        $importResult | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
    }
} else {
    Write-Host "[INFO] airflow-variables.json not found. Skipping auto-import." -ForegroundColor Yellow
    Write-Host "[TIP] Create airflow-variables.json and run: .\import-variables.ps1" -ForegroundColor Gray
}

# Lấy IP (thử nhiều cách)
$PUBLIC_IP = "localhost"
try {
    $response = Invoke-RestMethod -Uri "https://api.ipify.org" -TimeoutSec 5
    $PUBLIC_IP = $response
} catch {
    try {
        $response = Invoke-RestMethod -Uri "https://ifconfig.me" -TimeoutSec 5
        $PUBLIC_IP = $response.Trim()
    } catch {
        $PUBLIC_IP = "<YOUR-IP>"
    }
}

Write-Host ""
Write-Host "[OK] Airflow started successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "[WEB] Airflow UI: http://localhost:8081" -ForegroundColor Cyan
Write-Host "   (Or http://${PUBLIC_IP}:8081 if accessing remotely)" -ForegroundColor Gray
Write-Host "[USER] Username: admin" -ForegroundColor Cyan
Write-Host "[PASS] Password: admin" -ForegroundColor Cyan
Write-Host ""
Write-Host "[DAGS] Available DAGs:" -ForegroundColor Yellow
Write-Host "   - premier_league_batch_scrape (Daily at 6:00 AM)"
Write-Host "   - premier_league_backfill (Manual trigger)"
Write-Host "   - spark_mysql_transformation (Daily at 7:00 AM)"
Write-Host ""
Write-Host "[LOGS] To view logs: docker-compose -f docker-compose.airflow.yml logs -f" -ForegroundColor Gray
