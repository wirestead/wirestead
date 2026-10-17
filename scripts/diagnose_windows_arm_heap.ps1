param([int]$Repetitions = 500, [switch]$CaptureOnly)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
if ($Repetitions -lt 1 -or $Repetitions -gt 2000) { throw 'Repetitions must be 1..2000' }
$evidence = (New-Item -ItemType Directory -Force evidence).FullName
$dumps = (New-Item -ItemType Directory -Force evidence/dumps).FullName
$binary = @(Get-ChildItem build -Recurse -Filter run_integration_test_tcp_stop_admission.exe)
if ($binary.Count -ne 1) { throw 'Expected one TCP stop admission executable' }
$testBinary = $binary[0].FullName
$filter = 'ClientAndServer/TcpStopAdmissionTest.BothOutsideStopsWaitUntilCleanupActuallyCompletes/0'
Get-ComputerInfo | Select-Object WindowsProductName, WindowsVersion, OsVersion, OsArchitecture |
    ConvertTo-Json | Set-Content evidence/environment.json
Get-ChildItem Env: | Where-Object Name -Match '^(ImageOS|ImageVersion|RUNNER_OS|RUNNER_ARCH)$' |
    Format-List | Out-File evidence/runner-image.txt
Get-ChildItem $binary[0].Directory -File | Where-Object Extension -In '.exe', '.dll', '.pdb' |
    Get-FileHash -Algorithm SHA256 | ConvertTo-Json | Set-Content evidence/binary-hashes.json

$results = [System.Collections.Generic.List[object]]::new()
function Invoke-RepeatedTest([string]$mode) {
    for ($iteration = 1; $iteration -le $Repetitions; ++$iteration) {
        $report = Join-Path $evidence "$mode-$iteration.xml"
        & $testBinary "--gtest_filter=$filter" "--gtest_output=xml:$report" 2>&1 |
            Out-File -Append "evidence/$mode.log"
        $testExit = $LASTEXITCODE
        $validReport = $false
        if (Test-Path $report) {
            [xml]$xml = Get-Content $report -Raw
            $validReport = $xml.testsuites.tests -eq '1' -and $xml.testsuites.failures -eq '0' -and
                $xml.testsuites.disabled -eq '0'
        }
        $results.Add([pscustomobject]@{ mode=$mode; iteration=$iteration; exit_code=$testExit; valid_report=$validReport })
        $results | ConvertTo-Json | Set-Content evidence/results.json
        if ($testExit -ne 0 -or -not $validReport) { return $false }
    }
    return $true
}

# Preserve the original fresh-process execution pattern before attaching a
# debugger: instrumentation can suppress timing-sensitive teardown faults.
$plainPassed = $true
if (-not $CaptureOnly) { $plainPassed = Invoke-RepeatedTest 'plain' }

# The native ARM64 postmortem debugger captures unhandled exceptions without
# having to attach after the process has already terminated.
Invoke-WebRequest https://download.sysinternals.com/files/Procdump.zip -OutFile evidence/procdump.zip
Expand-Archive evidence/procdump.zip -DestinationPath evidence/procdump -Force
$procDump = Join-Path $evidence 'procdump/procdump64a.exe'
if (-not (Test-Path $procDump)) { throw 'Native ARM64 ProcDump is missing' }
Get-AuthenticodeSignature $procDump | Format-List | Out-File evidence/procdump-signature.txt
if ((Get-AuthenticodeSignature $procDump).Status -ne 'Valid') { throw 'Invalid ProcDump signature' }
Get-FileHash $procDump -Algorithm SHA256 | Format-List | Out-File evidence/procdump-hash.txt
$installed = $false
try {
    & $procDump -accepteula -ma -i $dumps 2>&1 | Out-File evidence/procdump-install.log
    if ($LASTEXITCODE -ne 0) { throw 'Postmortem debugger registration failed' }
    $installed = $true
    $capturedPassed = Invoke-RepeatedTest 'postmortem'
} finally {
    if ($installed) { & $procDump -accepteula -u 2>&1 | Out-File evidence/procdump-uninstall.log }
    Get-ChildItem $dumps -File | Select-Object Name, Length | ConvertTo-Json | Set-Content evidence/dump-inventory.json
}
if (-not $plainPassed -or -not $capturedPassed) { throw 'TCP stop admission reproduction failed; inspect evidence' }
