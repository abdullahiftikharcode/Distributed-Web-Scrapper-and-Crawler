@echo off
echo Starting database reset and services...

REM Stop all containers first
echo Stopping all running containers...
docker-compose down
timeout /t 5

REM Start all services again
echo Starting all services...
docker-compose up -d
timeout /t 10

REM Check if worker is up
echo Checking if worker is up...
curl -s http://localhost:5000/status > nul
if %errorlevel% neq 0 (
    echo Failed to connect to worker, retrying in 5 seconds...
    timeout /t 5
    curl -s http://localhost:5000/status > nul
    if %errorlevel% neq 0 (
        echo Worker is not available. Please check docker logs.
        exit /b 1
    )
)

REM Reset the database
echo Resetting database (clearing all URLs, logs, and queue data)...
curl -X POST http://localhost:5000/reset_all -H "Content-Type: application/json" -d "{\"confirm\": true}"
timeout /t 3

REM Start the worker
echo Starting worker...
curl -X POST http://localhost:5000/start -H "Content-Type: application/json" -d "{\"worker_id\": \"worker1\"}"
timeout /t 2

REM Add initial URL to crawl
echo Adding initial URL to crawl...
curl -X POST http://localhost:5001/add_job -H "Content-Type: application/json" -d "{\"url\": \"http://books.toscrape.com\"}"

echo.
echo All done! The services are running with a fresh database.
echo.

pause 