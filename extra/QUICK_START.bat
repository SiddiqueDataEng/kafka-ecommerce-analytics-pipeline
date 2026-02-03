@echo off
cls
echo.
echo  ██╗  ██╗ █████╗ ███████╗██╗  ██╗ █████╗ 
echo  ██║ ██╔╝██╔══██╗██╔════╝██║ ██╔╝██╔══██╗
echo  █████╔╝ ███████║█████╗  █████╔╝ ███████║
echo  ██╔═██╗ ██╔══██║██╔══╝  ██╔═██╗ ██╔══██║
echo  ██║  ██╗██║  ██║██║     ██║  ██╗██║  ██║
echo  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝
echo.
echo  🚀 STREAMING PIPELINE DASHBOARD
echo.
echo ========================================
echo   Welcome to the Kafka Pipeline Demo!
echo ========================================
echo.
echo This will start the interactive web dashboard
echo that demonstrates real-time event processing.
echo.
echo ✨ Features:
echo   • Real-time event generation
echo   • Stream processing simulation
echo   • Interactive charts and graphs
echo   • Live event monitoring
echo   • Beautiful responsive UI
echo.
echo 🌐 Dashboard URL: http://localhost:5000
echo.
echo Press any key to launch the dashboard...
pause >nul

echo.
echo 🚀 Starting dashboard...
call run_demo_ui.bat