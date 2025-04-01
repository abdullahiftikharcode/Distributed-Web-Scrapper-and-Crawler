# PowerShell script to reset the database and start all services
Write-Host "Starting database reset and services..." -ForegroundColor Green

# Stop all containers first
Write-Host "Stopping all running containers..." -ForegroundColor Yellow
docker-compose down
Start-Sleep -Seconds 5

# Start all services again
Write-Host "Starting all services..." -ForegroundColor Yellow
docker-compose up -d
Start-Sleep -Seconds 10

# Function to wait for a service
function Wait-For-Service {
    param (
        [string]$Url,
        [int]$MaxAttempts = 10,
        [int]$Delay = 2
    )
    
    Write-Host "Waiting for service at $Url..." -ForegroundColor Yellow
    $attempt = 1
    
    while ($attempt -le $MaxAttempts) {
        try {
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                Write-Host "Service at $Url is available" -ForegroundColor Green
                return $true
            }
            Write-Host "Attempt $attempt/$MaxAttempts: Service not ready (status code $($response.StatusCode))" -ForegroundColor Yellow
        }
        catch {
            Write-Host "Attempt $attempt/$MaxAttempts: Service not ready (connection error)" -ForegroundColor Yellow
        }
        
        $attempt++
        Start-Sleep -Seconds $Delay
    }
    
    Write-Host "Service at $Url did not become available after $MaxAttempts attempts" -ForegroundColor Red
    return $false
}

# Check if worker is up
if (-not (Wait-For-Service -Url "http://localhost:5000/status")) {
    Write-Host "Worker service did not become available. Please check docker logs." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit
}

# Reset the database
Write-Host "Resetting database (clearing all URLs, logs, and queue data)..." -ForegroundColor Yellow
try {
    $resetResponse = Invoke-RestMethod -Uri "http://localhost:5000/reset_all" -Method Post -Headers @{"Content-Type" = "application/json"} -Body '{"confirm": true}'
    Write-Host "Database reset successful: $($resetResponse.message)" -ForegroundColor Green
}
catch {
    Write-Host "Failed to reset database: $_" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit
}
Start-Sleep -Seconds 3

# Start the worker
Write-Host "Starting worker..." -ForegroundColor Yellow
try {
    $workerResponse = Invoke-RestMethod -Uri "http://localhost:5000/start" -Method Post -Headers @{"Content-Type" = "application/json"} -Body '{"worker_id": "worker1"}'
    Write-Host "Worker started successfully" -ForegroundColor Green
}
catch {
    Write-Host "Failed to start worker: $_" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit
}
Start-Sleep -Seconds 2

# Check if scheduler is up
if (-not (Wait-For-Service -Url "http://localhost:5001/status")) {
    Write-Host "Scheduler service did not become available. Please check docker logs." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit
}

# Add initial URL to crawl
Write-Host "Adding initial URL to crawl..." -ForegroundColor Yellow
try {
    $urlResponse = Invoke-RestMethod -Uri "http://localhost:5001/add_job" -Method Post -Headers @{"Content-Type" = "application/json"} -Body '{"url": "http://books.toscrape.com"}'
    Write-Host "Initial URL added successfully" -ForegroundColor Green
}
catch {
    Write-Host "Failed to add initial URL: $_" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit
}

Write-Host "`nAll done! The services are running with a fresh database." -ForegroundColor Green
Write-Host "Visit http://localhost:8501 to use the web interface." -ForegroundColor Cyan
Write-Host

Read-Host "Press Enter to exit" 