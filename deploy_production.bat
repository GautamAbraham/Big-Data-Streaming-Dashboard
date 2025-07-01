@echo off
REM Production deployment script for radiation monitoring system

echo ========================================
echo  Production Deployment - Scaled System
echo  4 vCPUs, 8GB RAM Configuration
echo ========================================

echo.
echo Stopping development environment...
docker-compose down --remove-orphans

echo.
echo Starting production environment...
docker-compose -f docker-compose.prod.yaml up --build -d

echo.
echo Waiting for services to initialize...
timeout /t 45 /nobreak

echo.
echo ========================================
echo  Production Health Checks
echo ========================================

echo Checking Kafka health...
timeout /t 30 /nobreak
docker-compose -f docker-compose.prod.yaml exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo.
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
echo Monitor at: http://localhost:8081 (Flink Dashboard)
echo Frontend: http://localhost:3000
echo Backend API: http://localhost:8000
echo Prometheus: http://localhost:9090
pause
