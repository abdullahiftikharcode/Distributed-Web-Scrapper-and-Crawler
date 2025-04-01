# Complete reset script that drops MongoDB databases, rebuilds all containers, and starts fresh
Write-Host "COMPLETE SYSTEM RESET" -ForegroundColor Red -BackgroundColor White
Write-Host "This will completely reset the system, drop all databases, and start fresh." -ForegroundColor Yellow
$confirmation = Read-Host "Are you sure you want to proceed? (yes/no)"
if ($confirmation -ne "yes") {
    Write-Host "Reset cancelled." -ForegroundColor Green
    exit
}

# Stop all containers
Write-Host "Stopping all containers..." -ForegroundColor Yellow
docker-compose down
Start-Sleep -Seconds 5

# Remove MongoDB volume to completely clear all data
Write-Host "Removing MongoDB volume..." -ForegroundColor Yellow
docker volume rm distributedwebscrapperandcrawler_mongodb_data
Start-Sleep -Seconds 2

# Rebuild all images
Write-Host "Rebuilding all images..." -ForegroundColor Yellow
docker-compose build
Start-Sleep -Seconds 5

# Start everything fresh
Write-Host "Starting all services with fresh data..." -ForegroundColor Yellow
docker-compose up -d
Start-Sleep -Seconds 15

# Function to wait for a service
function Wait-For-Service {
    param (
        [string]$Url,
        [string]$Name,
        [int]$MaxAttempts = 20,
        [int]$Delay = 2
    )
    
    Write-Host "Waiting for $Name at $Url..." -ForegroundColor Yellow
    $attempt = 1
    
    while ($attempt -le $MaxAttempts) {
        try {
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                Write-Host "$Name is available" -ForegroundColor Green
                return $true
            }
        }
        catch {
            Write-Host "Attempt $attempt/$MaxAttempts: $Name not ready yet" -ForegroundColor Yellow
        }
        
        $attempt++
        Start-Sleep -Seconds $Delay
    }
    
    Write-Host "$Name did not become available after $MaxAttempts attempts" -ForegroundColor Red
    return $false
}

# Wait for worker
if (-not (Wait-For-Service -Url "http://localhost:5000/status" -Name "Worker")) {
    Write-Host "Worker not available. Aborting." -ForegroundColor Red
    exit 1
}

# Start worker
Write-Host "Starting worker..." -ForegroundColor Yellow
try {
    $workerResponse = Invoke-RestMethod -Uri "http://localhost:5000/start" -Method Post -Headers @{"Content-Type" = "application/json"} -Body '{"worker_id": "worker1"}'
    Write-Host "Worker started successfully" -ForegroundColor Green
}
catch {
    Write-Host "Failed to start worker: $_" -ForegroundColor Red
    exit 1
}
Start-Sleep -Seconds 2

# Wait for scheduler
if (-not (Wait-For-Service -Url "http://localhost:5001/status" -Name "Scheduler")) {
    Write-Host "Scheduler not available. Aborting." -ForegroundColor Red
    exit 1
}

# Start scheduler
Write-Host "Starting scheduler..." -ForegroundColor Yellow
try {
    $schedulerResponse = Invoke-RestMethod -Uri "http://localhost:5001/start" -Method Post -Headers @{"Content-Type" = "application/json"} -Body '{}'
    Write-Host "Scheduler started successfully" -ForegroundColor Green
}
catch {
    Write-Host "Failed to start scheduler: $_" -ForegroundColor Red
    exit 1
}
Start-Sleep -Seconds 2

# Add initial URL to crawl
Write-Host "Adding initial URL to crawl..." -ForegroundColor Yellow
try {
    $urlResponse = Invoke-RestMethod -Uri "http://localhost:5001/add_job" -Method Post -Headers @{"Content-Type" = "application/json"} -Body '{"url": "http://books.toscrape.com"}'
    Write-Host "Initial URL added successfully: $($urlResponse.message)" -ForegroundColor Green
}
catch {
    Write-Host "Failed to add initial URL: $_" -ForegroundColor Red
    exit 1
}

Write-Host "`nSystem has been completely reset and initialized!" -ForegroundColor Green
Write-Host "Check the containers for logs to confirm processing is working." -ForegroundColor Cyan
Write-Host "Visit http://localhost:8501 to use the web interface." -ForegroundColor Cyan
Write-Host

Read-Host "Press Enter to exit" 