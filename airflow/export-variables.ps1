# PowerShell script để export Airflow Variables ra JSON file
# Usage: .\export-variables.ps1 [output-file.json]

param(
    [string]$OutputFile = "airflow-variables-exported.json"
)

Write-Host "[EXPORT] Exporting Airflow Variables to $OutputFile..." -ForegroundColor Yellow

# Kiểm tra container đang chạy
$containerRunning = docker ps --filter "name=airflow" --format "{{.Names}}" | Select-String "airflow"

if (-not $containerRunning) {
    Write-Host "[ERROR] Airflow container is not running!" -ForegroundColor Red
    exit 1
}

# Tạo Python script để export variables
$pythonScript = @"
import json
from airflow.models import Variable

variables = []
try:
    # Lấy tất cả variables
    all_vars = Variable.get_all()
    
    for key in all_vars:
        try:
            var = Variable.get(key, deserialize_json=False)
            variables.append({
                'key': key,
                'value': var,
                'description': ''
            })
        except:
            pass
    
    print(json.dumps({'variables': variables}, indent=2))
except Exception as e:
    print(f'ERROR: {e}')
"@

# Ghi script tạm
$pythonScript | Out-File -FilePath "export_vars_temp.py" -Encoding utf8

# Chạy trong container và lấy output
$jsonOutput = docker exec airflow python export_vars_temp.py 2>&1

# Xóa file tạm
Remove-Item "export_vars_temp.py" -ErrorAction SilentlyContinue

# Kiểm tra kết quả
if ($jsonOutput -match "ERROR") {
    Write-Host "[ERROR] Failed to export variables: $jsonOutput" -ForegroundColor Red
    exit 1
}

# Lưu vào file
try {
    $jsonOutput | Out-File -FilePath $OutputFile -Encoding utf8
    Write-Host "[OK] Variables exported to $OutputFile" -ForegroundColor Green
    
    # Parse và hiển thị số lượng
    $json = $jsonOutput | ConvertFrom-Json
    Write-Host "[INFO] Exported $($json.variables.Count) variables" -ForegroundColor Cyan
} catch {
    Write-Host "[ERROR] Failed to save file: $_" -ForegroundColor Red
    exit 1
}
