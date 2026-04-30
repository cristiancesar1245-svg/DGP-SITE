@echo off
setlocal
cd /d "%~dp0"

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-python-site.ps1"
if errorlevel 1 (
    echo.
    echo Nao foi possivel reiniciar o sistema. Veja a mensagem acima.
    pause
)

endlocal
