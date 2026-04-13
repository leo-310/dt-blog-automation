$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$nodeDir = Join-Path $root ".tools\node\node-v24.14.1-win-x64"
$npmCmd = Join-Path $nodeDir "npm.cmd"
$npmCache = Join-Path $root ".npm-cache"

if (-not (Test-Path $npmCmd)) {
    throw "Missing local Node runtime under .tools."
}

New-Item -ItemType Directory -Force $npmCache | Out-Null
$env:Path = "$nodeDir;$env:Path"
$env:npm_config_cache = $npmCache

if ($args.Count -eq 0) {
    & $npmCmd run dev
    exit $LASTEXITCODE
}

& $npmCmd @args
