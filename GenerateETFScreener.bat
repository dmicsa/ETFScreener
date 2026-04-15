@echo off
setlocal
cd /d "%~dp0"

deno run -A ".\Code\GenerateETFScreener.ts" %*
set "exit_code=%ERRORLEVEL%"

endlocal & exit /b %exit_code%