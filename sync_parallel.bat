@echo off
setlocal
cd /d C:\MorrisFiles\Proyectos\ControlTower

REM Cargar .env (con BOM) y setear PYTHONPATH
powershell -NoProfile -Command ^
  "$env:PYTHONPATH = (Get-Location).Path + '\src'; " ^
  "Get-Content .env | ForEach-Object { if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return } " ^
  "if ($_ -match '^\s*([^=]+)=(.*)$') { [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process') } } " ^
  "python scripts\run_sync_parallel.py"
endlocal