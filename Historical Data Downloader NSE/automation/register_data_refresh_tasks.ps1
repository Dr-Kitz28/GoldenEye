[CmdletBinding()]
param(
    [string]$PythonExe = "D:/Trading Strategies/.venv/Scripts/python.exe",
    [string]$DailyTime = "05:45",
    [string]$HourlyTime = "06:30",
    [int]$DailyWorkers = 6,
    [int]$HourlyWorkers = 6,
    [switch]$Unregister,
    [switch]$RunAfterRegister
)

$projectRoot = (Split-Path -Parent $PSScriptRoot)
$dailyScript = Join-Path $projectRoot "run_daily_pct_all.py"
$hourlyScript = Join-Path $projectRoot "run_hourly_all.py"
$taskNames = @{
    Daily = "TradingDataRefresh-DailyOHLC"
    Hourly = "TradingDataRefresh-Hourly1H"
}

function Test-RequiredFile {
    param(
        [Parameter(Mandatory=$true)][string]$Path,
        [Parameter(Mandatory=$true)][string]$Label
    )

    if (-not (Test-Path $Path)) {
        throw "Cannot locate $Label at '$Path'. Update the parameters and retry."
    }
}

function Remove-ExistingTask {
    param(
        [Parameter(Mandatory=$true)][string]$TaskName
    )

    $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -ne $existing) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }
}

function Register-RefreshTask {
    param(
        [Parameter(Mandatory=$true)][string]$TaskName,
        [Parameter(Mandatory=$true)][string]$TaskArgs,
        [Parameter(Mandatory=$true)][datetime]$RunAt,
        [Parameter(Mandatory=$true)][string]$Description
    )

    $action = New-ScheduledTaskAction -Execute $PythonExe -Argument $TaskArgs -WorkingDirectory $projectRoot
    $trigger = New-ScheduledTaskTrigger -Daily -At $RunAt
    $settings = New-ScheduledTaskSettingsSet -WakeToRun:$false -StartWhenAvailable:$true -AllowStartIfOnBatteries:$true -DontStopIfGoingOnBatteries:$true
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Description $Description -Force | Out-Null
}

try {
    Test-RequiredFile -Path $PythonExe -Label "Python executable"
    Test-RequiredFile -Path $dailyScript -Label "daily refresh script"
    Test-RequiredFile -Path $hourlyScript -Label "hourly refresh script"

    if ($Unregister) {
        Remove-ExistingTask -TaskName $taskNames.Daily
        Remove-ExistingTask -TaskName $taskNames.Hourly
        Write-Host "Scheduled tasks removed (if they existed)."
        return
    }

    $dailyArgs = '"{0}" --quiet --workers {1}' -f $dailyScript, $DailyWorkers
    $hourlyArgs = '"{0}" --quiet --workers {1} --lookback-days 5' -f $hourlyScript, $HourlyWorkers

    $dailyTimeParsed = [datetime]::ParseExact($DailyTime, "HH:mm", [System.Globalization.CultureInfo]::InvariantCulture)
    $hourlyTimeParsed = [datetime]::ParseExact($HourlyTime, "HH:mm", [System.Globalization.CultureInfo]::InvariantCulture)

    Remove-ExistingTask -TaskName $taskNames.Daily
    Remove-ExistingTask -TaskName $taskNames.Hourly

    Register-RefreshTask -TaskName $taskNames.Daily -TaskArgs $dailyArgs -RunAt $dailyTimeParsed -Description "Refresh NSE daily OHLCV + pct change datasets"
    Register-RefreshTask -TaskName $taskNames.Hourly -TaskArgs $hourlyArgs -RunAt $hourlyTimeParsed -Description "Refresh NSE hourly OHLCV datasets"

    Write-Host "Registered scheduled tasks:" -ForegroundColor Green
    Write-Host "  * $($taskNames.Daily) at $DailyTime"
    Write-Host "  * $($taskNames.Hourly) at $HourlyTime"

    if ($RunAfterRegister) {
        Start-ScheduledTask -TaskName $taskNames.Daily
        Start-ScheduledTask -TaskName $taskNames.Hourly
        Write-Host "Triggered both tasks for immediate execution."
    }
}
catch {
    Write-Error $_
    throw
}
