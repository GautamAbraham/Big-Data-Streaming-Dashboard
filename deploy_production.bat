@echo off
REM Production deployment script for radiation monitoring system

echo ========================================
echo  Production Deployment - Scaled System
echo  4 vCPUs, 8GB RAM Configuration
echo ========================================

echo.
echo Environment variables loaded from front_end/.env file
echo VITE_API_URL=http://localhost:8000
echo VITE_WS_URL=ws://localhost:8000/ws

echo.
echo Stopping development environment...
docker-compose down --remove-orphans

echo.
echo Starting production environment (dependencies handled automatically)...
docker-compose -f docker-compose.prod.yaml up --build -d

echo.
echo ========================================
echo  Production Health Checks
echo ========================================

echo Checking Backend health...
curl -f http://localhost:8000/health || echo Backend health check failed

echo.
echo ========================================
echo  Deploying Flink Job
echo ========================================

echo Submitting Flink processing job...
docker-compose -f docker-compose.prod.yaml exec flink-processor python /opt/flink/usrlib/flink_process.py

echo.
echo ========================================
echo  Production System Status
echo ========================================
docker-compose -f docker-compose.prod.yaml ps

echo.
echo Production deployment complete!
echo Docker Compose handled all service dependencies automatically.
echo.
echo ========================================
echo  Access URLs
echo ========================================
echo Frontend:       http://localhost:3000
echo Backend API:     http://localhost:8000
echo Flink Dashboard: http://localhost:8081
echo Prometheus:      http://localhost:9090
echo ========================================
pause
