$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonExe = Join-Path $root ".venv\Scripts\python.exe"

if (-not (Test-Path $pythonExe)) {
    throw "Missing virtual environment at .venv. Setup has not completed."
}

& $pythonExe -m blog_agent.cli @args
