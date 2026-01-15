# PowerShell script để import Airflow Variables từ JSON file
# Usage: .\import-variables.ps1 [variables-file.json]

param(
    [string]$VariablesFile = "airflow-variables.json"
)

Write-Host "[IMPORT] Importing Airflow Variables from $VariablesFile..." -ForegroundColor Yellow

# Kiểm tra file tồn tại
if (-not (Test-Path $VariablesFile)) {
    Write-Host "[ERROR] File $VariablesFile not found!" -ForegroundColor Red
    Write-Host "[INFO] Please create $VariablesFile first or specify path:" -ForegroundColor Yellow
    Write-Host "  .\import-variables.ps1 -VariablesFile path/to/variables.json" -ForegroundColor Gray
    exit 1
}

# Đọc JSON file
try {
    $json = Get-Content $VariablesFile -Raw | ConvertFrom-Json
} catch {
    Write-Host "[ERROR] Failed to parse JSON file: $_" -ForegroundColor Red
    exit 1
}

# Kiểm tra container đang chạy
$containerRunning = docker ps --filter "name=airflow" --format "{{.Names}}" | Select-String "airflow"

if (-not $containerRunning) {
    Write-Host "[ERROR] Airflow container is not running!" -ForegroundColor Red
    Write-Host "[INFO] Start Airflow first:" -ForegroundColor Yellow
    Write-Host "  docker-compose -f docker-compose.airflow.yml up -d" -ForegroundColor Gray
    exit 1
}

# Hỗ trợ cả 2 format JSON
$variables = @()
if ($json.PSObject.Properties.Name -contains 'variables') {
    # Format với array: {"variables": [{"key": "...", "value": "..."}]}
    $variables = $json.variables
} else {
    # Format đơn giản: {"key1": "value1", "key2": "value2"}
    $variables = $json.PSObject.Properties | ForEach-Object {
        @{
            key = $_.Name
            value = $_.Value
            description = ''
        }
    }
}

Write-Host "[INFO] Found $($variables.Count) variables to import" -ForegroundColor Cyan
Write-Host ""

$successCount = 0
$failCount = 0

# Import từng variable
foreach ($var in $variables) {
    $key = if ($var.key) { $var.key } else { $var.Name }
    $value = if ($var.value) { $var.value } else { $var.Value }
    $description = if ($var.description) { $var.description } else { '' }
    
    Write-Host "[SET] Setting variable: $key..." -ForegroundColor Yellow
    
    # Escape value để tránh lỗi shell
    $escapedValue = $value.ToString() -replace "'", "''" -replace '"', '\"'
    $escapedDesc = $description -replace "'", "''" -replace '"', '\"'
    
    # Tạo Python script để set variable
    $pythonScript = @"
from airflow.models import Variable
try:
    Variable.set('$key', '$escapedValue', description='$escapedDesc')
    print('OK')
except Exception as e:
    print(f'ERROR: {e}')
"@
    
    # Ghi script tạm
    $pythonScript | Out-File -FilePath "set_var_temp.py" -Encoding utf8
    
    # Chạy trong container
    $result = docker exec airflow python set_var_temp.py 2>&1
    
    if ($result -match "OK") {
        Write-Host "  [OK] $key = $value" -ForegroundColor Green
        $successCount++
    } else {
        Write-Host "  [FAIL] $key : $result" -ForegroundColor Red
        $failCount++
    }
    
    # Xóa file tạm
    Remove-Item "set_var_temp.py" -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "[SUMMARY]" -ForegroundColor Yellow
Write-Host "  Success: $successCount" -ForegroundColor Green
Write-Host "  Failed: $failCount" -ForegroundColor $(if ($failCount -gt 0) { "Red" } else { "Green" })

if ($successCount -gt 0) {
    Write-Host ""
    Write-Host "[INFO] Variables imported successfully!" -ForegroundColor Green
    Write-Host "[TIP] View variables in Airflow UI: Admin -> Variables" -ForegroundColor Cyan
}
