$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonExe = Join-Path $root ".venv\Scripts\python.exe"

if (-not (Test-Path $pythonExe)) {
    throw "Missing virtual environment at .venv. Setup has not completed."
}

Push-Location $root
try {
    & $pythonExe -c "import json; from blog_agent.api import BlogAgentApi; print(json.dumps(BlogAgentApi().automation_tick(force=False), ensure_ascii=False))"
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
