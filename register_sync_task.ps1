$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File C:\MorrisFiles\Proyectos\ControlTower\sync_parallel.ps1"

# Run every 2 hours between 07:00 and 21:00 (excluded)
$times = @("07:00","09:00","11:00","13:00","15:00","17:00","19:00")
$triggers = foreach ($t in $times) {
    New-ScheduledTaskTrigger -Daily -At $t
}

Register-ScheduledTask -TaskName "ControlTower Sync Parallel" -Action $action -Trigger $triggers -RunLevel Highest -Force
