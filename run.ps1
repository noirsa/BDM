# run.ps1
# Start the full stack with automatic GPU detection and 3 Spark workers.

param(
    [switch]$Prune,
    [switch]$DeepPrune
)

$ErrorActionPreference = "Stop"

function Test-NvidiaGpu {
    try {
        $null = Get-Command nvidia-smi -ErrorAction Stop
        nvidia-smi | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

if ($Prune) {
    Write-Host "Cleaning Docker build cache..." -ForegroundColor Yellow
    docker builder prune -f

    Write-Host "Cleaning dangling images..." -ForegroundColor Yellow
    docker image prune -f
}

if ($DeepPrune) {
    Write-Host "Deep cleaning unused Docker images and build cache. Volumes will NOT be removed." -ForegroundColor Yellow
    docker builder prune -a -f
    docker image prune -a -f
}

$HasGpu = Test-NvidiaGpu

if ($HasGpu) {
    Write-Host "NVIDIA GPU detected. Using GPU compose override." -ForegroundColor Cyan
    $ComposeFiles = @("-f", "docker-compose.yml", "-f", "docker-compose.gpu.yml")
}
else {
    Write-Host "No NVIDIA GPU detected. Using default non-GPU stack." -ForegroundColor Yellow
    $ComposeFiles = @("-f", "docker-compose.yml")
}

Write-Host ""
Write-Host "Step 1: Starting full stack without scaling Spark workers first..." -ForegroundColor Cyan
docker compose @ComposeFiles up -d

Write-Host ""
Write-Host "Step 2: GPU verification..." -ForegroundColor Cyan

if ($HasGpu) {
    Write-Host "Verifying CUDA in Jupyter..." -ForegroundColor Yellow
    docker compose @ComposeFiles exec jupyter python -c "import torch; print('CUDA available:', torch.cuda.is_available()); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'NO GPU')"
}
else {
    Write-Host "GPU verification skipped because no NVIDIA GPU was detected." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Step 3: Scaling Spark workers to 3..." -ForegroundColor Cyan
docker compose @ComposeFiles up -d --scale spark-worker=3

Write-Host ""
Write-Host "Full stack started." -ForegroundColor Green
Write-Host ""
Write-Host "Useful local URLs:"
Write-Host "  Jupyter          http://localhost:8888    token: jupyter"
Write-Host "  Airflow          http://localhost:8080    user/password: airflow / airflow"
Write-Host "  MinIO Console    http://localhost:9001    credentials from .env"
Write-Host "  Spark Master UI  http://localhost:8082    confirm Workers (3)"
Write-Host "  Kafka UI         http://localhost:8081"
Write-Host "  Mongo Express    http://localhost:8083"
Write-Host "  Attu             http://localhost:3000"
Write-Host "  Superset         http://localhost:8088"
Write-Host ""
Write-Host "Check Spark workers:"
Write-Host "  Open http://localhost:8082 and confirm Workers (3)"