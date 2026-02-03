@echo off
cls
echo.
echo ========================================
echo   Stopping Kafka Docker Containers
echo ========================================
echo.
echo 🛑 Stopping Kafka containers...
echo.

docker-compose -f docker-compose-kafka.yml down

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✅ Kafka containers stopped successfully!
    echo.
) else (
    echo.
    echo ❌ Error stopping containers.
    echo.
)

echo.
echo Press any key to continue...
pause >nul