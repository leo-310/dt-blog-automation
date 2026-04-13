@echo off
setlocal
set "ROOT=%~dp0"
if /I "%ROOT:~0,4%"=="\\?\" set "ROOT=%ROOT:~4%"
cd /d "%ROOT%"

if not exist ".venv\Scripts\python.exe" (
  echo Missing virtual environment at .venv. Setup has not completed.
  exit /b 1
)

if exist ".venv\Scripts\blog-agent-api.exe" (
  ".venv\Scripts\blog-agent-api.exe"
  exit /b %ERRORLEVEL%
)

".venv\Scripts\python.exe" -c "from blog_agent.api import main; main()"
exit /b %ERRORLEVEL%
