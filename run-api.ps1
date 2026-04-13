$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonExe = Join-Path $root ".venv\Scripts\python.exe"
$apiExe = Join-Path $root ".venv\Scripts\blog-agent-api.exe"

if (-not (Test-Path $pythonExe)) {
    throw "Missing virtual environment at .venv. Setup has not completed."
}

if (Test-Path $apiExe) {
    & $apiExe
    exit $LASTEXITCODE
}

& $pythonExe -c "from blog_agent.api import main; main()"
