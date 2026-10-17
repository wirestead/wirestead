# Serial DTR retention decision and HIL validation

Decision date: 2026-10-17. Related: [#710](https://github.com/wirestead/wirestead/issues/710),
[original HIL tests #711](https://github.com/wirestead/wirestead/pull/711),
[documentation correction #712](https://github.com/wirestead/wirestead/pull/712).

## Decision and compatibility

The user agreed to retain existing defaults and add an explicit DTR retention
option. The implementation is commit `6546bbddb5a739233e0c2d5bdb70b0e63b671e97`,
based on `2c1750ae92f95c71b9cea8a83d59e3307a335ee7`.

| Setting | Default | Explicit retention enabled |
| --- | --- | --- |
| `dtr` | `std::nullopt`, leave line untouched | Assert DTR; `dtr(false)` is invalid |
| `keep_dtr_on_close` | `false` | `true`, Linux only |
| `HUPCL` | Leave current tty setting untouched | Clear after open; persists after stop/destruction |
| `reopen_on_error` | `true` | Unchanged |
| `rx_idle_timeout_ms` | `0` (disabled) | Unchanged |

The builder, wrapper and native config expose the option. Driver refusal or an
unsupported platform produces a terminal connection error, without a retry
loop or a successful connection notification. The flag is not restored on
close because restoration would defeat retention. Disabling the option leaves
existing tty state untouched. Other users of the device may need to restore
HUPCL explicitly. See [tuning](tuning.md) for usage and restoration.

This does not promise to prevent reset on the first open, physical unplug,
power loss or a driver that ignores electrical DTR retention. It does not
change automatic reconnect defaults. Existing applications retain their
configuration behavior. Consumers must rebuild against the new headers and
library; pre-1.0 C++ ABI is not frozen (the config and internal serial interface
have additions).

## Environment

- Host: NVIDIA Jetson Orin Nano Super engineering reference developer kit.
- L4T: R36 revision 5.2; kernel `5.15.199-tegra`, native `aarch64`.
- Peer: Arduino UNO Mini, 16U2 USB bridge, Linux `cdc_acm`, 115200 baud.
- Port: `/dev/ttyACM0`; device ID
  `usb-Arduino__www.arduino.cc__Arduino_UNO_Mini_7513131383235170B101-if00`.
- Compiler: GCC 11.4.0; CMake 4.4.4; RelWithDebInfo; TLS off.
- Dependencies: Boost 1.91.0 from installed vcpkg tree
  `d015e31e90838a4c9dfa3eed45979bc70d9357fc`.
- Firmware: the existing rig sketch implements `!ping`, `!boots`, `!mute`,
  `!tick` and EEPROM boot counting. Its saved source SHA256 is
  `f4c277b225f30b93b0006076d78241141a5c52ab5e4a0dc1e8324efdc9014b35`;
  it differs from the repository sketch only by three flashing-instruction
  comment lines. No firmware was flashed during this validation.

## Results

Baseline source: `2c1750ae92f95c71b9cea8a83d59e3307a335ee7`.
All three existing HIL tests passed five consecutive executions each: **15/15**.
Candidate implementation: `6546bbddb5a739233e0c2d5bdb70b0e63b671e97`.
All four HIL tests passed five consecutive executions each: **20/20**.

| HIL scenario | Baseline | Candidate |
| --- | --- | --- |
| 200 echo round trips of 32 bytes | 5/5 | 5/5 |
| Idle timeout reopens a silent device | 5/5 | 5/5 |
| `dtr(false)` still cannot prevent reset at open | 5/5 | 5/5 |
| Explicit retention preserves boot count through stop/start and idle reopen | Not present | 5/5 |

The new scenario establishes HUPCL enabled before opening the tested port,
then enables retention and queries the EEPROM boot count. It stops and
restarts the same wrapper, mutes the peer for 3500 ms, waits for the 2500 ms
receive watchdog to reopen the port (100 ms retry interval), synchronizes
with `!ping`, and queries the boot count again. Each execution observed three
connections with an unchanged counter: **184→184, 185→185, 186→186,
187→187, 188→188**. The test restores the original HUPCL bit afterward.

The related transport, config mapping and builder tests passed **146/146**
(135 + 5 + 6), including default opt-out, conflicting settings in both setter
orders, and driver refusal without reconnect retries. No test was skipped.

## Commands and evidence

```sh
cmake --build /home/jorin/followup-20261017/build --target \
  run_integration_test_serial_hil run_unit_transport_serial \
  run_unit_wrapper_serial_config_mapping \
  run_integration_test_serial_builder_validation -j2
WIRESTEAD_HIL_SERIAL=/dev/ttyACM0 ctest \
  --test-dir /home/jorin/followup-20261017/build \
  -L hil --repeat until-fail:5 -j1 -V
```

Each of the three focused test executables was also run directly with
`--gtest_output=xml:<evidence-path>`. `git diff --check` and clang-format's
check mode passed for the twelve changed C++ files.

Log copies below have trailing whitespace trimmed; original logs remain in
the workspace evidence directory.

- [Baseline HIL log](evidence/20261017/serial-hil-baseline.log)
- [Candidate HIL log](evidence/20261017/serial-hil-retention.log)
- [Counts, boot counters and focused test results](evidence/20261017/serial-results.json)
- Focused result XML: [transport](evidence/20261017/run_unit_transport_serial.xml),
  [mapping](evidence/20261017/run_unit_wrapper_serial_config_mapping.xml),
  [builder](evidence/20261017/run_integration_test_serial_builder_validation.xml)
- [Baseline/candidate binary and candidate library hashes](evidence/20261017/serial-binary-hashes.txt)

The first baseline invocation used `/dev/serial/by-id/...`; existing device
path validation rejected it before opening. The completed baseline and
candidate runs both used `/dev/ttyACM0`. An initial incremental candidate link
used an older wrapper object because extracted source mtimes preceded the
baseline object timestamps. Source timestamps were refreshed and all four
targets rebuilt successfully before the candidate tests above. These failed
setup attempts were retained in the workspace evidence directory and are not
counted as passing tests.

This validates the stated board/driver/kernel combination. No other board,
macOS/Windows retention implementation, unplug recovery test, release or merge
is claimed here.
