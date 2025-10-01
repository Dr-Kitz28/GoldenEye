$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

$venvPython = Join-Path $projectRoot "..\.venv\Scripts\python.exe"
$pythonExe = if (Test-Path $venvPython) { $venvPython } else { "python" }

$outputPath = Join-Path $projectRoot "MASTEK_hourly_latest.csv"
$metadataPath = Join-Path $projectRoot "MASTEK_hourly_latest_metadata.json"

$arguments = @(
    "mastek_timeInterval_OHLCV_data.py",
    "--symbol", "MASTEK.NS",
    "--interval", "1h",
    "--output", $outputPath,
    "--metadata", $metadataPath,
    "--incremental",
    "--extended-hourly",
    "--no-progress"
)

& $pythonExe @arguments
