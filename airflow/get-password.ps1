# PowerShell script để lấy mật khẩu Airflow từ logs
# Usage: .\get-password.ps1

Write-Host "[INFO] Getting Airflow admin password from logs..." -ForegroundColor Yellow
Write-Host ""

# Kiểm tra container có đang chạy không
$containerRunning = docker ps --filter "name=airflow" --format "{{.Names}}" | Select-String "airflow"

if (-not $containerRunning) {
    Write-Host "[ERROR] Airflow container is not running!" -ForegroundColor Red
    Write-Host "[INFO] Start Airflow first:" -ForegroundColor Yellow
    Write-Host "  .\start-airflow.ps1" -ForegroundColor Gray
    exit 1
}

# Lấy password từ logs
Write-Host "[SEARCH] Searching for password in Airflow logs..." -ForegroundColor Cyan

# Tìm password trong logs (Airflow standalone in password khi khởi động)
$passwordLine = docker logs airflow 2>&1 | Select-String -Pattern "Password for user 'admin'" | Select-Object -First 1

if ($passwordLine) {
    Write-Host ""
    Write-Host "[FOUND] Password found in logs!" -ForegroundColor Green
    Write-Host ""
    
    # Extract password từ dòng log
    # Format: "Password for user 'admin': wA4PZAxKN5p3UEkX"
    $passwordMatch = $passwordLine.Line -match "Password for user 'admin':\s+(\S+)"
    
    if ($passwordMatch) {
        $adminPassword = $matches[1]
        Write-Host ("=" * 60) -ForegroundColor Cyan
        Write-Host "[CREDENTIALS] Airflow Admin Login:" -ForegroundColor Yellow
        Write-Host ("=" * 60) -ForegroundColor Cyan
        Write-Host "Username: admin" -ForegroundColor White
        Write-Host "Password: $adminPassword" -ForegroundColor Green
        Write-Host ("=" * 60) -ForegroundColor Cyan
        Write-Host ""
        Write-Host "[WEB] Airflow UI: http://localhost:8081" -ForegroundColor Cyan
    } else {
        Write-Host "[INFO] Full password line:" -ForegroundColor Yellow
        Write-Host $passwordLine.Line -ForegroundColor Gray
    }
} else {
    Write-Host "[WARNING] Password not found in recent logs." -ForegroundColor Yellow
    Write-Host "[INFO] Searching in full logs..." -ForegroundColor Cyan
    
    # Thử tìm trong toàn bộ logs
    $fullPasswordLine = docker logs airflow 2>&1 | Select-String -Pattern "Password for user" | Select-Object -Last 1
    
    if ($fullPasswordLine) {
        Write-Host ""
        Write-Host "[FOUND] Password found:" -ForegroundColor Green
        Write-Host $fullPasswordLine.Line -ForegroundColor Yellow
    } else {
        Write-Host "[INFO] Try viewing full logs:" -ForegroundColor Cyan
        Write-Host "  docker logs airflow" -ForegroundColor Gray
        Write-Host ""
        Write-Host "[INFO] Or check Airflow UI - password might be displayed on first login page" -ForegroundColor Cyan
    }
}

Write-Host ""
Write-Host "[TIP] Default credentials (if set manually):" -ForegroundColor Yellow
Write-Host "  Username: admin" -ForegroundColor Cyan
Write-Host "  Password: admin (or check logs above)" -ForegroundColor Cyan
Write-Host ""
Write-Host "[LOGS] To view full logs:" -ForegroundColor Gray
Write-Host "  docker logs airflow" -ForegroundColor DarkGray
Write-Host "  docker logs airflow --tail 100" -ForegroundColor DarkGray
