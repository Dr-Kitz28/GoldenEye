# Windows Task Scheduler Setup

Automate hourly and daily refreshes of the Yahoo Finance datasets using the helper PowerShell scripts in `automation/`.

## 1. Update the helper scripts if needed
- `automation\update_hourly.ps1` keeps `MASTEK_hourly_latest.csv` and metadata current using the incremental mode of `mastek_timeInterval_OHLCV_data.py`.
- `automation\update_daily.ps1` appends the latest daily bar to `MASTEK_daily_latest.csv` using the incremental mode of `mastek_historical_data.py`.
- Both scripts assume the project virtual environment lives at `..\.venv\Scripts\python.exe`. Adjust `$venvPython` inside the scripts if you relocate the environment.

## 2. Create the scheduled tasks (run once as Administrator)
Open **Windows PowerShell** as Administrator and run:

```powershell
$repo = "D:\Trading Strategies\historical_data_downloader"
$hourlyScript = Join-Path $repo "automation\update_hourly.ps1"
$dailyScript  = Join-Path $repo "automation\update_daily.ps1"

schtasks /Create /
    TN "MASTEK Hourly OHLCV Update" /
    SC HOURLY /
    MO 1 /
    RL HIGHEST /
    TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$hourlyScript`"" /
    ST 00:15 /
    F

schtasks /Create /
    TN "MASTEK Daily OHLCV Update" /
    SC DAILY /
    ST 20:30 /
    RL HIGHEST /
    TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$dailyScript`"" /
    F
```

- Adjust `ST` (start time) to suit your trading hours. Example above runs hourly refresh starting at 00:15 and the daily job at 20:30 local time.
- The hourly task runs every 60 minutes; Task Scheduler automatically restarts the hourly cadence from the specified start time.
- `RL HIGHEST` ensures the task can access the `.venv` even if stored under your user profile.

## 3. Verify and monitor
- Once the tasks are registered, use `schtasks /Query /TN "MASTEK Hourly OHLCV Update" /V /FO LIST` to confirm the schedule.
- Check `%SystemRoot%\System32\Tasks` to confirm the XML entries if needed.
- Task history is available in the Task Scheduler GUI (enable **All Tasks History** in the right-hand Actions pane).

## 4. Manual test run
Before relying on automation, execute one run manually to confirm credentials and paths:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "D:\Trading Strategies\historical_data_downloader\automation\update_hourly.ps1"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "D:\Trading Strategies\historical_data_downloader\automation\update_daily.ps1"
```

Both scripts log to the console. For unattended runs, redirect output to a log file by editing the scheduled-task `TR` command to append `>> "D:\Logs\mastek_hourly.log" 2>&1`.

---

With these tasks in place, `MASTEK_hourly_latest.csv` will update every hour and `MASTEK_daily_latest.csv` will refresh once per day without manual intervention.
