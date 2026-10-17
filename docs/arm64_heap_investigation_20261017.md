# Windows ARM64 heap corruption investigation

Evidence date: 2026-10-17. This is the Windows ARM64 failure recorded during
[#713](https://github.com/wirestead/wirestead/pull/713), not the separate
historical Orin/Linux `epoll_reactor` SIGSEGV or the #714 bootstrap HTTP 504s.

## Issue, expected and actual behavior

`ClientAndServer/TcpStopAdmissionTest.BothOutsideStopsWaitUntilCleanupActuallyCompletes/false`
parks transport cleanup, starts two outside `stop()` callers, confirms both
wait, releases cleanup, then restarts and stops the same native TCP client.
It should complete without corrupting process memory.

The [original job](https://github.com/wirestead/wirestead/actions/runs/38118047733/job/114406860425)
failed with exit code `0xc0000374` during that test on 2026-10-11. It was the
only failure among 2598 enabled smoke tests. The
[same-source rerun](https://github.com/wirestead/wirestead/actions/runs/38118047733/job/114412939732)
passed; that did not establish that the defect had been corrected.

## Reproduction and environment

Source head: `c96135070a61d07752f6d703366a3189d6972bf6`.
The preserved binary comes from attempt 2 of run `38118047733`; the failed
attempt's build artifact was replaced by the workflow's overwrite policy.
The preserved artifact has no crash dump or PDB. Runtime, tests, CMake,
`VCPKG_BASELINE` and `ci.yml` have no differences between this source and main
`2c1750ae92f95c71b9cea8a83d59e3307a335ee7`.

Original environment: native Windows ARM64, Windows 11 build `10.0.26200`,
`windows-11-vs2026-arm64` image `20261004.176.1`, MSVC `14.51.36231`,
Release, Boost 1.92.0, triplet `arm64-windows`.

New reproduction: same source revision's preserved executable and DLLs, native
`windows-11-arm`, OS build `10.0.26200`, image `20261013.183.4`. The image is
newer than the original. Windows' product-name field reports a legacy
"Windows 10 Enterprise" value; the OS build and runner image identify the
actual environment. This is not a claim that the original VM was retained.

Preserved executable SHA256:
`6e74610262433771b1b60d4599b58ace22f7fdf2d62caf1d4e809d9085339170`.
Each repetition launches a fresh process with this filter:

```text
--gtest_filter=ClientAndServer/TcpStopAdmissionTest.BothOutsideStopsWaitUntilCleanupActuallyCompletes/0
```

The `/false` CTest parameter is `/0` in the Google Test executable filter.
The harness checks both the process exit status and XML proving exactly one
non-disabled test executed successfully. It stops a phase on its first failure
and preserves the result; it never converts a failed attempt into success.

## Results and dumps

[Diagnostic run 38986794870](https://github.com/wirestead/wirestead/actions/runs/38986794870)
reproduced the original heap-corruption exit code:

| Mode | Executions | Outcome |
| --- | --- | --- |
| Fresh processes, no postmortem debugger registered | 360 | 359 passed; execution 360 exited `0xc0000374` without a completed XML report |
| ProcDump 12.02 ARM64 registered as postmortem debugger | 500 | 500 passed; no dump generated |

The run correctly concluded **failure**. ProcDump's signature and SHA256,
registration/unregistration logs, environment, all completed XML reports,
per-execution results and binary hashes are in the
[diagnostic artifact](https://github.com/wirestead/wirestead/actions/runs/38986794870/artifacts/12082858358),
retained for 30 days. The workspace also contains a downloaded copy.
The original failure happened before debugger registration, so it has no dump.
No crash occurred during the 500 capture-enabled repetitions.

The second bounded capture-only experiment,
[run 38987309420](https://github.com/wirestead/wirestead/actions/runs/38987309420),
used diagnostic commit `df62cfeeac6f1f8d05c0dbf20e2914ba3d7a39bf` and stopped
at its first failure: **87 passed; execution 88 exited `0xc0000374`**.
The maximum was 2000 executions; it did not run the remaining 1912 after the
failure. This is a second reproduction on native Windows ARM64. ProcDump
postmortem registration succeeded, but no dump was present in the uploaded
[second artifact](https://github.com/wirestead/wirestead/actions/runs/38987309420/artifacts/12083109893).

**Dump collection remains a gap.** The existing capture mechanism did not
provide a dump even when the process failed. The record does not identify
whether Windows bypassed the postmortem debugger, collection had not completed
before artifact upload, or another runner setting prevented it. The next
focused experiment should monitor process launch directly (`procdump -ma -e -x`)
or validate WER dump collection before increasing the repetition budget.

Local evidence copies:

- [Reproduction summary](evidence/20261017/windows-arm-reproduction-summary.json)
- [All 860 per-execution outcomes](evidence/20261017/windows-arm-native-results.json)
- [Second capture phase outcomes](evidence/20261017/windows-arm-capture-results.json)
- [Native environment](evidence/20261017/windows-arm-native-environment.json)
- [Failure log tail](evidence/20261017/windows-arm-failure-tail.log)

## Cause and affected surface

**Root cause remains unknown.** Reproduction confirms that the heap-corruption
risk remains on this source; the successful original rerun and the 500 passes
with capture enabled do not rule it out.

The smallest relevant surface is the native TCP client's two-caller stop
completion and same-object restart path, including canceled resolve/connect
handlers, their lifetime guards and cleanup publication. The test uses a
managed external `io_context`. These are investigation boundaries, not a
confirmed faulty allocation/free or a reason to apply a speculative patch.

The serial option tracked separately in #710 is unrelated, and the baseline PR only changes the
vcpkg pin. A green baseline CI run is not evidence that this TCP heap error is
fixed. No runtime fix, disabled test, relaxed assertion or hidden retry has
been added for this investigation.

## Patch release judgment

A patch claiming to fix this heap corruption is **not ready**. Preserve the
Windows ARM64 risk as unresolved until a dump or other direct evidence
identifies the fault and a focused change passes repeated regression on the
same native Windows ARM64 toolchain/dependency configuration. Any decision to
release independent fixes must explicitly retain this known risk; this record
does not authorize or publish a release.

Capture budget: the initial 360/500 experiment plus one capture-only
experiment capped at 2000 executions and stopped at failure 88. The bounded
investigation is complete with two reproductions, no dump and an explicit
unresolved-risk decision. Further work should correct and validate dump
collection before repeating or attempting a runtime fix.

The workflow is now manual-only to avoid repeating this bounded experiment on
report updates. It is pinned to the original run's artifact, whose original
retention period still applies. Preserve or repoint that input before a later
experiment; the uploaded diagnostic artifacts independently retain the tested
executables and DLLs for 30 days.
