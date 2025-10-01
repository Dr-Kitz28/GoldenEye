$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

$venvPython = Join-Path $projectRoot "..\.venv\Scripts\python.exe"
$pythonExe = if (Test-Path $venvPython) { $venvPython } else { "python" }

$outputPath = Join-Path $projectRoot "MASTEK_daily_latest.csv"
$metadataPath = Join-Path $projectRoot "MASTEK_daily_latest_metadata.json"

$arguments = @(
    "mastek_historical_data.py",
    "--symbol", "MASTEK.NS",
    "--interval", "1d",
    "--output", $outputPath,
    "--metadata", $metadataPath,
    "--incremental",
    "--quiet"
)

& $pythonExe @arguments
