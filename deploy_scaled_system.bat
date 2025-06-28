@echo off
echo ========================================
echo    Deploying Scaled Radiation Monitoring System
echo    Target: 4 vCPUs, 8 GB RAM
echo ========================================

echo.
echo [1/6] Stopping any existing containers...
docker-compose down
docker container prune -f

echo.
echo [2/6] Building updated images...
docker-compose build

echo.
echo [3/6] Starting core services (Kafka)...
docker-compose up -d kafka

echo.
echo [4/6] Waiting for Kafka to be ready...
timeout /t 30 /nobreak

echo.
echo [5/6] Starting Flink cluster...
docker-compose up -d flink-jobmanager flink-taskmanager-1 flink-taskmanager-2

echo.
echo [6/6] Starting all remaining services...
docker-compose up -d

echo.
echo ========================================
echo System Status Check
echo ========================================
docker-compose ps

echo.
echo ========================================
echo Resource Allocation Summary:
echo ========================================
echo JobManager:      1.0 CPU, 1536MB RAM
echo TaskManager 1:   2.0 CPU, 1536MB RAM (2 slots)
echo TaskManager 2:   2.0 CPU, 1536MB RAM (2 slots)
echo Kafka:          0.5 CPU, 1024MB RAM
echo Backend:        0.3 CPU,  512MB RAM
echo Frontend:       0.2 CPU,  256MB RAM
echo Data Provider:  0.2 CPU,  256MB RAM
echo ----------------------------------------
echo Total:          6.2 CPU, 6.5GB RAM
echo Available:      4.0 CPU, 8.0GB RAM
echo Total Parallelism: 4 slots available
echo ========================================

echo.
echo Deployment complete! 
echo - Flink Dashboard: http://localhost:8081
echo - Backend API: http://localhost:8000
echo - Frontend: http://localhost:3000
echo.
echo To submit the Flink job:
echo docker exec -it jobmanager ./bin/flink run -d /opt/flink/usrlib/flink_process.py
pause
