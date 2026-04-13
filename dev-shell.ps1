$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$nodeDir = Join-Path $root ".tools\node\node-v24.14.1-win-x64"
$pythonExe = Join-Path $root ".venv\Scripts\python.exe"
$npmCache = Join-Path $root ".npm-cache"

if (-not (Test-Path $pythonExe)) {
    throw "Missing virtual environment at .venv. Setup has not completed."
}

if (-not (Test-Path $nodeDir)) {
    throw "Missing local Node runtime under .tools."
}

New-Item -ItemType Directory -Force $npmCache | Out-Null
$env:Path = "$nodeDir;$env:Path"
$env:npm_config_cache = $npmCache
$env:VIRTUAL_ENV = Join-Path $root ".venv"

Write-Host "Local blog-agent shell ready."
Write-Host "Python: $pythonExe"
Write-Host "Node:   $nodeDir"
Write-Host ""
Write-Host "Examples:"
Write-Host "  .\\.venv\\Scripts\\blog-agent.exe --help"
Write-Host "  npm run dev"
Write-Host "  npm run build"

