# ==============================================================================
#  Apache James Server - Graceful Background Starter (Portable & Completely Silent)
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
$SILO_BIN      = "$ROOT_DIR\soft\Silo_260916"
$SILO_DATA     = "$ROOT_DIR\data\silo"
$JAVA_CMD      = "$ROOT_DIR\soft\JRE_25\bin\java.exe"
$JAMES_DIR     = "$ROOT_DIR\james"

$LOG_DIR = "$ROOT_DIR\logs"
if (!(Test-Path $LOG_DIR)) { New-Item -ItemType Directory -Path $LOG_DIR -Force | Out-Null }

# Helper function to launch process completely hidden via WMI Win32_Process
function Start-HiddenProcess {
    param(
        [Parameter(Mandatory=$true)][string]$CommandLine,
        [Parameter(Mandatory=$true)][string]$WorkingDirectory
    )
    $procStartup = [wmiclass]"Win32_ProcessStartup"
    $startupConfig = $procStartup.CreateInstance()
    $startupConfig.ShowWindow = 0 # 0 = SW_HIDE (Never show window)

    $procClass = [wmiclass]"Win32_Process"
    $res = $procClass.Create($CommandLine, $WorkingDirectory, $startupConfig)
    return $res.ReturnValue
}

# ---------------------------------------------------------------------------
# Step 1: Silo S3 (Background Daemon, WebUI disabled)
# ---------------------------------------------------------------------------
$siloProc = Get-Process -Name silo -ErrorAction SilentlyContinue
if (-not $siloProc) {
    $env:MINIO_ROOT_USER     = "minioadmin"
    $env:MINIO_ROOT_PASSWORD = "minioadmin"
    $env:MINIO_BROWSER       = "off"
    $env:GOMEMLIMIT          = "1073741824"   # 1 GiB = 1024 MiB — жёсткий лимит Go runtime (GOMEMLIMIT, Go 1.19+)
    $env:GODEBUG             = "madvdontneed=1"
    $env:MINIO_API_REQUESTS_MAX = "1600"
    $env:MINIO_DRIVE_SYNC    = "off"
    $siloCmd = "cmd.exe /c `"`"$SILO_BIN\silo.exe`" server `"$SILO_DATA`" --address :9000 >> `"$LOG_DIR\silo.log`" 2>&1`""
    Start-HiddenProcess -CommandLine $siloCmd -WorkingDirectory $SILO_DATA | Out-Null
}

# Wait for Silo S3 port 9000
$s3Ready = $false
for ($i = 1; $i -le 30; $i++) {
    try {
        $res = Invoke-WebRequest -Uri "http://127.0.0.1:9000/" -UseBasicParsing -TimeoutSec 1 -ErrorAction Stop
        if ($res.StatusCode -eq 403 -or $res.StatusCode -eq 200) {
            $s3Ready = $true
            break
        }
    } catch {
        # S3 responds with 403 on root without auth, which means service is up!
        if ($_.Exception.Response -and ($_.Exception.Response.StatusCode.value__ -eq 403 -or $_.Exception.Response.StatusCode.value__ -eq 200)) {
            $s3Ready = $true
            break
        }
        Start-Sleep -Seconds 1
    }
}

if (-not $s3Ready) {
    exit 1
}

# Ensure S3 bucket 'james-blobs' directory exists in Silo data
if (!(Test-Path "$SILO_DATA\james-blobs")) {
    New-Item -ItemType Directory -Path "$SILO_DATA\james-blobs" -Force | Out-Null
}

# ---------------------------------------------------------------------------
# Step 2: PostgreSQL 17 (Portable via pg_ctl)
# ---------------------------------------------------------------------------
# Dynamically detect logical CPU cores to align PostgreSQL workers with host capacity
$cpuCores = [System.Environment]::ProcessorCount
if ($cpuCores -lt 2) { $cpuCores = 2 }
$parallelGather = [Math]::Max(2, [int]($cpuCores / 2))

# Dynamic thread & pool calculations:
# Netty EventLoop threads: 24 (4x cores for 6 cores)
$nettyThreads = [Math]::Min(32, [Math]::Max(16, $cpuCores * 4))

# Spooler threads: 16 (2.67x cores, clamped between 8 and 32)
$spoolerThreads = [Math]::Min(32, [Math]::Max(8, [int]([Math]::Round($cpuCores * 2.67))))

# James Connection Pools:
# by-pass-rls is linked 1:1 with Netty threads (24) so each event loop worker has a dedicated connection slot
$bypassPool = $nettyThreads
# Main pool: 4 slots per spooler thread (16 * 4 = 64 slots) for full parallel database persistence
$slotsPerSpoolerThread = 4
$mainPool = $spoolerThreads * $slotsPerSpoolerThread

# PostgreSQL max_connections allocation:
# mainPool (64) + bypassPool (24) + superuser_reserved (5) + background workers & headroom (10) = 103
$pgSuperuserReserved = 5
$pgHeadroom = 10
$totalMaxConnections = $mainPool + $bypassPool + $pgSuperuserReserved + $pgHeadroom

$pgConfPath = "$POSTGRES_DATA\postgresql.conf"
if (Test-Path $pgConfPath) {
    $pgConf = Get-Content $pgConfPath -Raw -Encoding UTF8
    $newPgConf = $pgConf
    $newPgConf = $newPgConf -replace 'max_worker_processes = \d+', "max_worker_processes = $cpuCores"
    $newPgConf = $newPgConf -replace 'max_parallel_workers = \d+', "max_parallel_workers = $cpuCores"
    $newPgConf = $newPgConf -replace 'max_parallel_workers_per_gather = \d+', "max_parallel_workers_per_gather = $parallelGather"
    $newPgConf = $newPgConf -replace 'max_parallel_maintenance_workers = \d+', "max_parallel_maintenance_workers = $parallelGather"
    $newPgConf = $newPgConf -replace 'max_connections = \d+', "max_connections = $totalMaxConnections"
    $newPgConf = $newPgConf -replace 'superuser_reserved_connections = \d+', "superuser_reserved_connections = $pgSuperuserReserved"
    if ($newPgConf -ne $pgConf) {
        $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
        [System.IO.File]::WriteAllText($pgConfPath, $newPgConf, $utf8NoBom)
    }
}

& "$POSTGRES_BIN\pg_isready.exe" -h 127.0.0.1 -p 5432 -U postgres | Out-Null
if ($LASTEXITCODE -ne 0) {
    $pgCmd = "`"$POSTGRES_BIN\pg_ctl.exe`" -D `"$POSTGRES_DATA`" -l `"$LOG_DIR\postgres.log`" start"
    Start-HiddenProcess -CommandLine $pgCmd -WorkingDirectory $POSTGRES_DATA | Out-Null
}

$pgReady = $false
for ($i = 1; $i -le 20; $i++) {
    & "$POSTGRES_BIN\pg_isready.exe" -h 127.0.0.1 -p 5432 -U postgres | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $pgReady = $true
        break
    }
    Start-Sleep -Seconds 1
}

if (-not $pgReady) {
    exit 1
}

# ---------------------------------------------------------------------------
# Step 3: Apache James (Daemon with Generational ZGC)
# ---------------------------------------------------------------------------
$javaProc = Get-Process -Name java -ErrorAction SilentlyContinue
if (-not $javaProc) {
    $mailetXmlPath = "$JAMES_DIR\conf\mailetcontainer.xml"
    if (Test-Path $mailetXmlPath) {
        $xmlContent = Get-Content $mailetXmlPath -Raw -Encoding UTF8
        $updatedXml = $xmlContent -replace '<threads>\d+</threads>', "<threads>$spoolerThreads</threads>"
        if ($updatedXml -ne $xmlContent) {
            Set-Content -Path $mailetXmlPath -Value $updatedXml -Encoding UTF8
        }
    }

    # Dynamically scale PostgreSQL connection pool:
    # by-pass-rls is linked to Netty threads (24) so each event loop worker has a dedicated connection slot
    # main pool is scaled for spooler threads (48 slots)
    $pgPropPath = "$JAMES_DIR\conf\postgres.properties"
    if (Test-Path $pgPropPath) {
        $pgProp = Get-Content $pgPropPath -Raw -Encoding UTF8
        $newPgProp = $pgProp
        $newPgProp = $newPgProp -replace 'pool(\.initial\.size|\.initialSize)=\d+', "pool.initialSize=$mainPool"
        $newPgProp = $newPgProp -replace 'pool(\.max\.size|\.maxSize)=\d+', "pool.maxSize=$mainPool"
        $newPgProp = $newPgProp -replace 'by-pass-rls\.pool(\.initial\.size|\.initialSize)=\d+', "by-pass-rls.pool.initialSize=$bypassPool"
        $newPgProp = $newPgProp -replace 'by-pass-rls\.pool(\.max\.size|\.maxSize)=\d+', "by-pass-rls.pool.maxSize=$bypassPool"
        if ($newPgProp -ne $pgProp) {
            $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
            [System.IO.File]::WriteAllText($pgPropPath, $newPgProp, $utf8NoBom)
        }
    }

    $jvmOpts = "-Xms4096m -Xmx4096m -XX:+UseZGC -XX:ZAllocationSpikeTolerance=5 -XX:+AlwaysPreTouch -XX:+UseNUMA -XX:+UseCompactObjectHeaders -XX:+UseStringDeduplication -Djava.net.preferIPv4Stack=true -Dio.netty.tryReflectionSetAccessible=true -Dio.netty.allocator.type=pooled -Dio.netty.allocator.pageSize=65536 -Dio.netty.allocator.maxOrder=10 -Dio.netty.allocator.useCacheForAllThreads=true -Dio.netty.allocator.numDirectArenas=$nettyThreads -Dio.netty.eventLoopThreads=$nettyThreads -Dio.netty.tcpNoDelay=true -Dio.netty.leakDetection.level=DISABLED -Dio.netty.recycler.maxCapacityPerThread=4096 -Dactivemq.artemis.client.global.cleaner.interval=60000 -Djdk.nio.maxCachedBufferSize=262144 -Dnetworkaddress.cache.ttl=3600 -Dsun.net.inetaddr.ttl=3600 -Duser.timezone=UTC -Dworking.directory=. -Dlogback.configurationFile=conf/logback.xml"
    $javaCmd = "`"$JAVA_CMD`" $jvmOpts -jar james-server-postgres-app.jar"
    Start-HiddenProcess -CommandLine $javaCmd -WorkingDirectory $JAMES_DIR | Out-Null
}

# Wait for Apache James components to report HEALTHY (up to 40s)
$jamesReady = $false
for ($i = 1; $i -le 30; $i++) {
    try {
        $res = Invoke-RestMethod -Uri "http://127.0.0.1:8000/healthcheck" -TimeoutSec 2 -ErrorAction Stop
        if ($res.checks) {
            $jamesReady = $true
            break
        }
    } catch {}
    Start-Sleep -Seconds 1
}

if (-not $jamesReady) {
    exit 1
}

exit 0
