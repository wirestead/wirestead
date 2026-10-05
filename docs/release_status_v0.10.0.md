# v0.10.0 release status

Checked on **2026-10-03**. This is a dated publication snapshot, separate from
the reusable [release checklist](release_checklist.md) and the
[policy conformance report](communication_contract_v0.10_status.md).
A merged packaging PR, a successful publisher and an installable registry
package are different evidence stages.

## Published and merged

| Surface | Verified state | Evidence |
| --- | --- | --- |
| C++ core | v0.10.0 published on 2026-10-02 at `97789a4ca9940bc233e5e3b49b32daaca277b21a`; eight release assets uploaded | [Release](https://github.com/wirestead/wirestead/releases/tag/v0.10.0), [release workflow](https://github.com/wirestead/wirestead/actions/runs/36971248059) |
| Python | 0.10.0 pins core v0.10.0; GitHub release published and PyPI publisher job succeeded | [Pin/release PR #74](https://github.com/wirestead/wirestead-python/pull/74), [release](https://github.com/wirestead/wirestead-python/releases/tag/v0.10.0), [PyPI publication and wheel consumer checks](https://github.com/wirestead/wirestead-python/actions/runs/36975163725) |
| vcpkg | Port update to 0.10.0 merged; removes the obsolete Unilink config fixup | [microsoft/vcpkg#54243](https://github.com/microsoft/vcpkg/pull/54243) |
| Examples | Source pin advanced to v0.10.0 | [PR #20](https://github.com/wirestead/wirestead-examples/pull/20) |
| Container | Default core ref advanced to v0.10.0; image workflow succeeded | [PR #17](https://github.com/wirestead/wirestead-container/pull/17), [workflow](https://github.com/wirestead/wirestead-container/actions/runs/36973649073) |
| ROS adapter workspace | Core source pin advanced to v0.10.0; adapter remains at 0.1.0 | [PR #16](https://github.com/wirestead/wirestead-ros/pull/16) |
| Documentation | Removed-API examples corrected; subsequent Pages deployment succeeded | [PR #24](https://github.com/wirestead/wirestead-docs/pull/24), [Pages](https://github.com/wirestead/wirestead-docs/actions/runs/36974151588) |
| Benchmark | Results published for the exact v0.10.0 core tag on Jetson Orin Nano Super | [Benchmark release](https://github.com/wirestead/wirestead-benchmarks/releases/tag/benchmark-wirestead-v0.10.0) |

The Python publication statement above is based on the successful publisher
job, not an independent PyPI download/install during this documentation review.
The wheel consumer checks run before publication; they do not prove a later
registry download.

## Downstream distribution still to verify

| Surface | Verified state | Remaining evidence or dependency |
| --- | --- | --- |
| ROS Jazzy | Bloom release metadata for 0.10.0-1 merged in [ros/rosdistro#54339](https://github.com/ros/rosdistro/pull/54339) | Verify build-farm results, installation from `ros-testing`, then availability after the distribution sync separately |
| ROS Humble | Bloom release metadata for 0.10.0-2 merged in [ros/rosdistro#54340](https://github.com/ros/rosdistro/pull/54340) | Verify build-farm results, installation from `ros-testing`, then availability after the distribution sync separately |
| Conan Center | New recipe updated to 0.10.0 in the still-open [PR #30653](https://github.com/conan-io/conan-center-index/pull/30653); latest scheduler check requires maintainer action | Approval, recipe CI and merge remain pending; this is not a published Conan Center package |

ROS metadata merge does not establish binary availability. The adapter pin
update likewise does not mean a new `wirestead_ros` release was needed or made.

## Contract and performance boundaries

The approved built-in transport policies are implemented and shipped, with
scope and regression evidence in the [conformance report](communication_contract_v0.10_status.md).
The contract remains **Draft**: release publication does not promote any
Proposed/Open rule to a guarantee. Universal no-inline callbacks and unknown
custom-executor dependencies remain outside the completed scope. Rich Python
result objects are optional future API work; Python's bool adapter is complete.

Performance completion is also separate from publication. [PR #703](https://github.com/wirestead/wirestead/pull/703)
records remaining UDS idle-p99 target misses, including 120.504% of v0.9.6 in
the 1 KiB nanosecond-sample audit against a 120% target. Its 24-target matrix
covers TCP throughput and unloaded TCP/UDS p99, not full-sweep UDS throughput
or saturated-load latency. [PR #705](https://github.com/wirestead/wirestead/pull/705)
narrows the throughput claim accordingly.
