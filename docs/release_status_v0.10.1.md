# v0.10.1 release status

Checked on **2026-10-29**. This is a dated publication snapshot, separate from
the reusable [release checklist](release_checklist.md). A merged packaging PR,
a successful publisher and an installable registry package are different
evidence stages.

v0.10.1 is a bug-fix release: the `TcpClient::get_executor()` race behind the
Windows ARM64 heap corruption (#717), the exit-time segfault (#613), and the
opt-in serial `keep_dtr_on_close()` (#710). No API was removed or changed.

## Published and merged

| Surface | Verified state | Evidence |
| --- | --- | --- |
| C++ core | v0.10.1 published on 2026-10-29 at `88e9ab4761ff95d096e79d59446629a1ba026600`; eight release assets uploaded | [Release](https://github.com/wirestead/wirestead/releases/tag/v0.10.1), [release workflow](https://github.com/wirestead/wirestead/actions/runs/40861698688), [dry-run](https://github.com/wirestead/wirestead/actions/runs/40799238469) |
| #717 fix | Page heap located the use-after-free in `get_executor()`; ThreadSanitizer failed on the race before the fix and passed after; the Windows ARM64 reproduction ran 8,000 times without a failure after it | [PR #720](https://github.com/wirestead/wirestead/pull/720), [TSan before](https://github.com/wirestead/wirestead/actions/runs/40791732397), [TSan after](https://github.com/wirestead/wirestead/actions/runs/40791736913), [ARM64 diagnostic](https://github.com/wirestead/wirestead/actions/runs/40791742302) |
| Python | 0.10.1 pins core v0.10.1, the first release with CPython 3.14 wheels; 21 files on PyPI. The wheel jobs check out core `refs/tags/v0.10.1`, and a fresh 3.14 venv installs and imports 0.10.1 from PyPI | [Pin/release PR #83](https://github.com/wirestead/wirestead-python/pull/83), [release](https://github.com/wirestead/wirestead-python/releases/tag/v0.10.1), [PyPI publication](https://github.com/wirestead/wirestead-python/actions/runs/40891272845), [TestPyPI](https://github.com/wirestead/wirestead-python/actions/runs/40867189872) |
| Examples | Source pin and CI core checkout advanced to v0.10.1 | [PR #22](https://github.com/wirestead/wirestead-examples/pull/22) |
| Container | Default core ref advanced to v0.10.1; image workflow succeeded | [PR #18](https://github.com/wirestead/wirestead-container/pull/18), [workflow](https://github.com/wirestead/wirestead-container/actions/runs/40867087778) |
| ROS adapter workspace | Core source pin advanced to v0.10.1; adapter remains at 0.1.0 | [PR #17](https://github.com/wirestead/wirestead-ros/pull/17), [CI](https://github.com/wirestead/wirestead-ros/actions/runs/40867102772) |
| Documentation | `keep_dtr_on_close()` documented, held until the release was published; Pages deployment succeeded. `check_docs_coverage.sh v0.10.0` reports nothing undocumented. `check_docs_compile.sh` against the tag compiled 218 blocks and flagged one, the illustrative `MockSerialPort` in the contributor architecture guide's testing section, unchanged since the docs migration | [PR #26](https://github.com/wirestead/wirestead-docs/pull/26), [Pages](https://github.com/wirestead/wirestead-docs/actions/runs/40867118934) |
| ROS bloom | 0.10.1-1 released for Jazzy and Humble; rosdistro metadata merged | [Jazzy #54940](https://github.com/ros/rosdistro/pull/54940), [Humble #54941](https://github.com/ros/rosdistro/pull/54941) |
| Benchmark | Results published for the exact v0.10.1 core tag on Jetson Orin Nano Super. Against v0.10.0, 32 of 34 throughput cells and every latency p50 are within 10%; TCP besteffort 256 B (72%) and reliable 1024 B (70%) read lower, inside the 82-142 and 327-495 MiB/s spread of v0.10.0's own repeats of those cells | [Benchmark release](https://github.com/wirestead/wirestead-benchmarks/releases/tag/benchmark-wirestead-v0.10.1), [workflow](https://github.com/wirestead/wirestead-benchmarks/actions/runs/40967046021) |

## Downstream distribution still to verify

| Surface | Verified state | Remaining evidence or dependency |
| --- | --- | --- |
| ROS Jazzy and Humble | Metadata merged at 12:32 UTC; at 14:37 UTC the build farm had not yet rebuilt and `ros-testing` still served 0.10.0 | Verify build-farm results, installation from `ros-testing`, then availability after the distribution sync |
| vcpkg | Port update to 0.10.1 open, CI green | Review and merge of [microsoft/vcpkg#54758](https://github.com/microsoft/vcpkg/pull/54758); then the examples README's tested vcpkg version |
| Conan Center | New recipe updated to 0.10.1 in the still-open [PR #30653](https://github.com/conan-io/conan-center-index/pull/30653); scheduler check requires maintainer action | Approval, recipe CI and merge remain pending; this is not a published Conan Center package |

Neither registry PR was built locally: the vcpkg `git-tree` was computed with
`git rev-parse` rather than `x-add-version`, and the Conan recipe was not run
through `conan create`. Upstream changed nothing under `cmake/` and only the
version in `CMakeLists.txt`, so both carry only a version and a hash.
