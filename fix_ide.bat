@echo off
echo ===================================================
echo Fixing Antigravity IDE Cache and Resetting Handshake
echo ===================================================

echo [1/3] Closing Antigravity IDE...
taskkill /F /IM "Antigravity IDE.exe" >nul 2>&1
taskkill /F /IM "antigravity.exe" >nul 2>&1
timeout /t 2 /nobreak >nul

echo [2/3] Cleaning expired TLS certificates and caches...
del /q /f "%TEMP%\exebox-cert-*.crt" >nul 2>&1
rmdir /s /q "%APPDATA%\Antigravity IDE\Cache" >nul 2>&1
rmdir /s /q "%APPDATA%\Antigravity IDE\Code Cache" >nul 2>&1
rmdir /s /q "%APPDATA%\Antigravity IDE\GPUCache" >nul 2>&1

echo [3/3] Relaunching Antigravity IDE...
start "" "%LOCALAPPDATA%\Programs\Antigravity IDE\Antigravity IDE.exe"

echo ===================================================
echo Antigravity IDE has been restarted!
echo Note: If your top-right badge still shows 0 credits,
echo please click on the 0 badge to renew your plan/quota.
echo ===================================================
pause
