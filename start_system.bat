@echo off
REM Quick startup script for development

echo ========================================
echo  Quick Start - Development Environment
echo ========================================

echo.
echo Starting all services...
docker-compose up --build -d

echo.
echo Waiting for services to start...
timeout /t 30 /nobreak

echo.
echo ========================================
echo  Access Information
echo ========================================
echo Frontend:          http://localhost:3000
echo Backend API:       http://localhost:8000
echo Flink Dashboard:   http://localhost:8081
echo Kafka:             localhost:9092

echo.
echo ========================================
echo  Next Steps
echo ========================================
echo 1. Submit the Flink job:
echo    docker-compose exec flink-processor python /opt/flink/usrlib/flink_process.py
echo.
echo 2. Check logs:
echo    docker-compose logs -f [service-name]

echo.
echo System startup complete!
pause
