# PowerShell script để dừng Airflow trên Windows
# Usage: .\stop-airflow.ps1

Write-Host "[STOP] Stopping Airflow containers..." -ForegroundColor Yellow

docker-compose -f docker-compose.airflow.yml down

Write-Host ""
Write-Host "[OK] Airflow stopped successfully!" -ForegroundColor Green
