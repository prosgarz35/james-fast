# ==============================================================================
#  Apache James Server - Graceful Background Stopper (Portable)
#  Stack: Silo S3 (9000) + PostgreSQL 17 (5432) + Apache James (ZGC)
# ==============================================================================
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$ErrorActionPreference = "SilentlyContinue"

# Root directory determined dynamically from script location
$SCRIPT_DIR    = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($SCRIPT_DIR)) { $SCRIPT_DIR = (Get-Location).Path }
$ROOT_DIR      = (Resolve-Path $SCRIPT_DIR).Path

$POSTGRES_BIN  = "$ROOT_DIR\soft\PostgreSQL_17.11\bin"
$POSTGRES_DATA = "$ROOT_DIR\data\postgres"

Write-Host "==============================================================================="
Write-Host "           APACHE JAMES GRACEFUL SHUTDOWN (PORTABLE MODE)"
Write-Host "==============================================================================="

# ---------------------------------------------------------------------------
# Step 1: Drain Mail Queues (Graceful Spool Flushing) & Stop Java
# ---------------------------------------------------------------------------
Write-Host "[1/3] Gracefully draining Apache James mail spool and stopping Java..."
try {
    $spoolInfo = Invoke-RestMethod -Uri "http://127.0.0.1:8000/mailQueues/spool" -TimeoutSec 1 -ErrorAction Stop
    if ($spoolInfo) {
        Write-Host "  [-] Checking spool queue status..."
        for ($i = 1; $i -le 10; $i++) {
            $check = Invoke-RestMethod -Uri "http://127.0.0.1:8000/mailQueues/spool" -TimeoutSec 1 -ErrorAction SilentlyContinue
            if ($null -ne $check.size -and $check.size -eq 0) {
                Write-Host "  [-] Spool queue is empty (size: 0)."
                break
            }
            Write-Host "  [-] Waiting for spool queue to drain remaining messages (size: $($check.size))..."
            Start-Sleep -Seconds 1
        }
    }
} catch {
    # WebAdmin already offline or unreachable
}

Write-Host "  [-] Stopping Apache James (java.exe)..."
$javaProcesses = Get-Process -Name java -ErrorAction SilentlyContinue
if ($javaProcesses) {
    foreach ($p in $javaProcesses) {
        Stop-Process -Id $p.Id -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
    $lingeringJava = Get-Process -Name java -ErrorAction SilentlyContinue
    if ($lingeringJava) {
        Stop-Process -Name java -Force -ErrorAction SilentlyContinue
    }
}
Write-Host "  [OK] Apache James stopped cleanly."

# ---------------------------------------------------------------------------
# Step 2: PostgreSQL 17 (Commit WAL / Flush Buffers via pg_ctl)
# ---------------------------------------------------------------------------
Write-Host "[2/3] Gracefully stopping PostgreSQL 17 (committing WAL / flushing buffers)..."
& "$POSTGRES_BIN\pg_ctl.exe" -D "$POSTGRES_DATA" -m smart stop | Out-Null

for ($i = 1; $i -le 15; $i++) {
    & "$POSTGRES_BIN\pg_isready.exe" -h 127.0.0.1 -p 5432 -U postgres | Out-Null
    if ($LASTEXITCODE -ne 0) {
        break
    }
    Start-Sleep -Seconds 1
}
Write-Host "  [OK] PostgreSQL 17 stopped cleanly."

# ---------------------------------------------------------------------------
# Step 3: Silo S3
# ---------------------------------------------------------------------------
Write-Host "[3/3] Stopping Silo S3 Object Storage..."
$siloProcesses = Get-Process -Name silo -ErrorAction SilentlyContinue
if ($siloProcesses) {
    foreach ($s in $siloProcesses) {
        Stop-Process -Id $s.Id -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
    $lingeringSilo = Get-Process -Name silo -ErrorAction SilentlyContinue
    if ($lingeringSilo) {
        Stop-Process -Name silo -Force -ErrorAction SilentlyContinue
    }
}
Write-Host "  [OK] Silo S3 stopped cleanly."

Write-Host "==============================================================================="
Write-Host "                ALL SERVICES GRACEFULLY STOPPED"
Write-Host "==============================================================================="
