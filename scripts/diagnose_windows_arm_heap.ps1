param(
    [int]$Repetitions = 2000,
    [ValidateSet('selftest', 'plain', 'wer', 'procdump')][string[]]$Modes = @('selftest', 'wer', 'procdump')
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
if ($Repetitions -lt 1 -or $Repetitions -gt 5000) { throw 'Repetitions must be 1..5000' }
$evidence = (New-Item -ItemType Directory -Force evidence).FullName
$dumps = (New-Item -ItemType Directory -Force evidence/dumps).FullName
$binary = @(Get-ChildItem build -Recurse -Filter run_integration_test_tcp_stop_admission.exe)
if ($binary.Count -ne 1) { throw 'Expected one TCP stop admission executable' }
$testBinary = $binary[0].FullName
$imageName = $binary[0].Name
$filter = 'ClientAndServer/TcpStopAdmissionTest.BothOutsideStopsWaitUntilCleanupActuallyCompletes/0'
Get-ComputerInfo | Select-Object WindowsProductName, WindowsVersion, OsVersion, OsArchitecture |
    ConvertTo-Json | Set-Content evidence/environment.json
Get-ChildItem Env: | Where-Object Name -Match '^(ImageOS|ImageVersion|RUNNER_OS|RUNNER_ARCH)$' |
    Format-List | Out-File evidence/runner-image.txt
Get-ChildItem $binary[0].Directory -File | Where-Object Extension -In '.exe', '.dll', '.pdb' |
    Get-FileHash -Algorithm SHA256 | ConvertTo-Json | Set-Content evidence/binary-hashes.json

# Record why earlier capture may have been bypassed: WER policy, the AeDebug
# registration and the WER service state as the runner image ships them.
$werRoot = 'HKLM\SOFTWARE\Microsoft\Windows\Windows Error Reporting'
$aeDebug = 'HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\AeDebug'
& reg query $werRoot /s 2>&1 | Out-File evidence/wer-config-before.txt
& reg query $aeDebug 2>&1 | Out-File evidence/aedebug-before.txt
Get-Service WerSvc | Select-Object Name, Status, StartType | Format-List | Out-File evidence/wersvc-before.txt

# Heap corruption terminates through fail-fast, which skips unhandled
# exception filters. A dump may land after the process exit is observed,
# so wait until WER has finished and the newest dump stops growing.
function Wait-Dump([datetime]$since) {
    $deadline = (Get-Date).AddSeconds(120)
    $last = -1
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 3
        $dump = Get-ChildItem $dumps -Recurse -Filter *.dmp | Where-Object LastWriteTime -ge $since |
            Sort-Object LastWriteTime | Select-Object -Last 1
        $busy = @(Get-Process WerFault, WerFaultSecure -ErrorAction SilentlyContinue).Count -gt 0
        if ($dump -and -not $busy -and $dump.Length -eq $last) { return $dump }
        if ($dump) { $last = $dump.Length }
    }
    return $null
}

$results = [System.Collections.Generic.List[object]]::new()
function Invoke-RepeatedTest([string]$mode, [scriptblock]$launch) {
    for ($iteration = 1; $iteration -le $Repetitions; ++$iteration) {
        $report = Join-Path $evidence "$mode-$iteration.xml"
        $started = Get-Date
        $testExit = & $launch $report
        $validReport = $false
        if (Test-Path $report) {
            [xml]$xml = Get-Content $report -Raw
            $validReport = $xml.testsuites.tests -eq '1' -and $xml.testsuites.failures -eq '0' -and
                $xml.testsuites.disabled -eq '0'
            # Passing reports are reproducible from results.json; keep failures only.
            if ($testExit -eq 0 -and $validReport) { Remove-Item $report }
        }
        $result = [ordered]@{ mode=$mode; iteration=$iteration; exit_code=$testExit; valid_report=$validReport }
        if ($testExit -ne 0 -or -not $validReport) {
            $dump = Wait-Dump $started
            $result.dump = if ($dump) { $dump.FullName.Substring($evidence.Length + 1) } else { $null }
            $results.Add([pscustomobject]$result)
            $results | ConvertTo-Json | Set-Content evidence/results.json
            return $false
        }
        $results.Add([pscustomobject]$result)
        if ($iteration % 100 -eq 0) { $results | ConvertTo-Json | Set-Content evidence/results.json }
    }
    $results | ConvertTo-Json | Set-Content evidence/results.json
    return $true
}

$plainLaunch = {
    param($report)
    & $testBinary "--gtest_filter=$filter" "--gtest_output=xml:$report" 2>&1 | Out-File -Append evidence/plain.log
    $LASTEXITCODE
}

# WER LocalDumps captures fail-fast terminations without attaching a debugger,
# so the fresh-process timing that reproduced the fault is unchanged.
# Returns a script block that restores the previous WER policy.
function Enable-WerLocalDumps([string]$image, [string]$folder) {
    $key = "$werRoot\LocalDumps\$image"
    $disabled = (Get-ItemProperty "Registry::$werRoot" -Name Disabled -ErrorAction SilentlyContinue).Disabled
    New-Item -ItemType Directory -Force $folder | Out-Null
    & reg add $key /v DumpFolder /t REG_EXPAND_SZ /d $folder /f | Out-Null
    & reg add $key /v DumpType /t REG_DWORD /d 2 /f | Out-Null
    & reg add $key /v DumpCount /t REG_DWORD /d 10 /f | Out-Null
    & reg add $werRoot /v Disabled /t REG_DWORD /d 0 /f | Out-Null
    & reg add $werRoot /v DontShowUI /t REG_DWORD /d 1 /f | Out-Null
    Set-Service WerSvc -StartupType Manual
    Start-Service WerSvc
    & reg query $key 2>&1 | Out-File -Append evidence/wer-localdumps.txt
    {
        & reg delete $key /f 2>&1 | Out-Null
        if ($null -eq $disabled) { & reg delete $werRoot /v Disabled /f 2>&1 | Out-Null }
        else { & reg add $werRoot /v Disabled /t REG_DWORD /d $disabled /f | Out-Null }
    }.GetNewClosure()
}

function Invoke-WerMode {
    $restore = Enable-WerLocalDumps $imageName (Join-Path $dumps 'wer')
    try {
        Invoke-RepeatedTest 'wer' { param($report)
            & $testBinary "--gtest_filter=$filter" "--gtest_output=xml:$report" 2>&1 | Out-File -Append evidence/wer.log
            $LASTEXITCODE
        }
    } finally {
        & $restore
    }
}

function Get-ProcDump {
    $tool = Join-Path $evidence 'procdump/procdump64a.exe'
    if (Test-Path $tool) { return $tool }
    Invoke-WebRequest https://download.sysinternals.com/files/Procdump.zip -OutFile evidence/procdump.zip
    Expand-Archive evidence/procdump.zip -DestinationPath evidence/procdump -Force
    if (-not (Test-Path $tool)) { throw 'Native ARM64 ProcDump is missing' }
    Get-AuthenticodeSignature $tool | Format-List | Out-File evidence/procdump-signature.txt
    if ((Get-AuthenticodeSignature $tool).Status -ne 'Valid') { throw 'Invalid ProcDump signature' }
    Get-FileHash $tool -Algorithm SHA256 | Format-List | Out-File evidence/procdump-hash.txt
    $tool
}

# ProcDump launches the test itself and writes a dump on the unhandled or
# fail-fast exception. _NO_DEBUG_HEAP keeps the release heap a debugger-started
# process would otherwise replace with the debug heap.
function Invoke-ProcDumpMode {
    $tool = Get-ProcDump
    $pdDumps = (New-Item -ItemType Directory -Force (Join-Path $dumps 'procdump')).FullName
    $env:_NO_DEBUG_HEAP = '1'
    try {
        Invoke-RepeatedTest 'procdump' { param($report)
            $out = & $tool -accepteula -ma -e -x $pdDumps $testBinary "--gtest_filter=$filter" "--gtest_output=xml:$report" 2>&1
            $out | Out-File -Append evidence/procdump.log
            # ProcDump's own exit status is not the test's; take the reported one.
            $line = $out | Select-String 'exit code (-?\w+)' | Select-Object -Last 1
            # [long]: 0xC0000374 does not fit [int]. The XML check still decides.
            if (-not $line) { return 0 }
            $code = $line.Matches[0].Groups[1].Value
            if ($code -match '^0x') { [Convert]::ToInt64($code.Substring(2), 16) } else { [long]$code }
        }
    } finally {
        Remove-Item Env:_NO_DEBUG_HEAP -ErrorAction SilentlyContinue
    }
}

# A capture mechanism is only evidence once it has caught a known crash. Build
# a deliberate heap corruption, which the release heap reports as the same
# 0xc0000374 fail-fast, and require a dump from both capture paths. A small
# double free alone was tolerated (run 39511435537 exited 0), so the crasher
# overwrites a block header on a private heap without the low-fragmentation
# front end and exits 3 if the heap still does not detect it.
function Invoke-SelfTest {
    $dir = (New-Item -ItemType Directory -Force (Join-Path $evidence 'selftest')).FullName
    $src = Join-Path $dir 'heap_crash.c'
    $exe = Join-Path $dir 'heap_crash.exe'
    Set-Content $src @'
#include <string.h>
#include <windows.h>
int main(void) {
  HeapSetInformation(NULL, HeapEnableTerminationOnCorruption, NULL, 0);
  HANDLE heap = HeapCreate(0, 0, 0);
  unsigned char* block = (unsigned char*)HeapAlloc(heap, 0, 256);
  unsigned char* next = (unsigned char*)HeapAlloc(heap, 0, 256);
  memset(block, 0x41, 256 + 64);
  HeapFree(heap, 0, next);
  HeapFree(heap, 0, block);
  HeapValidate(heap, 0, NULL);
  return 3;
}
'@
    $vs = & "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe" -latest -property installationPath
    & cmd /c "`"$vs\VC\Auxiliary\Build\vcvarsall.bat`" arm64 >nul && cl /nologo /O2 `"$src`" /Fe:`"$exe`" /Fo:`"$dir\\`"" 2>&1 |
        Out-File evidence/selftest-build.log
    if (-not (Test-Path $exe)) { throw 'Self-test crasher did not build' }

    $outcome = [ordered]@{}
    $restore = Enable-WerLocalDumps 'heap_crash.exe' (Join-Path $dumps 'selftest-wer')
    try {
        $started = Get-Date
        & $exe
        $outcome.wer_exit = '0x{0:x8}' -f $LASTEXITCODE
        $outcome.wer_dump = $null -ne (Wait-Dump $started)
    } finally {
        & $restore
    }
    $tool = Get-ProcDump
    $pdDumps = (New-Item -ItemType Directory -Force (Join-Path $dumps 'selftest-procdump')).FullName
    & $tool -accepteula -ma -e -x $pdDumps $exe 2>&1 | Out-File evidence/selftest-procdump.log
    $outcome.procdump_dump = @(Get-ChildItem $pdDumps -Filter *.dmp).Count -gt 0
    $outcome | ConvertTo-Json | Set-Content evidence/selftest.json
    $outcome.wer_dump -and $outcome.procdump_dump
}

$passed = $true
foreach ($mode in $Modes) {
    $modePassed = switch ($mode) {
        'selftest' { Invoke-SelfTest }
        'plain' { Invoke-RepeatedTest 'plain' $plainLaunch }
        'wer' { Invoke-WerMode }
        'procdump' { Invoke-ProcDumpMode }
    }
    if (-not $modePassed) { $passed = $false; break }
}

Get-ChildItem $dumps -Recurse -File | Select-Object @{n='Name'; e={$_.FullName.Substring($evidence.Length + 1)}}, Length |
    ConvertTo-Json | Set-Content evidence/dump-inventory.json

# First-pass triage on the runner: exception record and stack with Microsoft
# public symbols. The preserved binary has no PDB, so its frames stay offsets.
$cdb = Get-ChildItem "${env:ProgramFiles(x86)}\Windows Kits\10\Debuggers\arm64\cdb.exe" -ErrorAction SilentlyContinue
foreach ($dump in Get-ChildItem $dumps -Recurse -Filter *.dmp) {
    if (-not $cdb) { 'cdb.exe not found on this image' | Out-File evidence/triage-missing.txt; break }
    & $cdb.FullName -z $dump.FullName -y 'srv*C:\symbols*https://msdl.microsoft.com/download/symbols' `
        -c '.exr -1; .ecxr; kn 64; !heap -s; ~*kn 32; lmv; q' 2>&1 | Out-File "evidence/triage-$($dump.BaseName).txt"
}
if (-not $passed) { throw 'TCP stop admission reproduction failed; inspect evidence' }
