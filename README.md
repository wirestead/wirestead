![Wirestead](assets/wirestead-logo-horizontal.svg#gh-light-mode-only)
![Wirestead](assets/wirestead-logo-dark.svg#gh-dark-mode-only)

# Wirestead™

**Robust, simple async communication for modern C++20.**

Serial · TCP · UDP · UDS — one API for all four, on Linux, macOS and Windows, x64 and arm64.

![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows%20%7C%20macOS-informational)
![vcpkg](https://img.shields.io/badge/vcpkg-wirestead-0078D6)
[![Coverage](https://img.shields.io/endpoint?url=https://wirestead.github.io/wirestead-docs/coverage/badges/coverage.json)](https://wirestead.github.io/wirestead-docs/coverage/)

## Description

`wirestead` provides a unified interface for asynchronous communication across different transports, allowing applications to switch between Serial, TCP, UDP, and UDS with minimal code changes. The public C++ API exposes builders and wrappers for all four transport families.

The project prioritizes **API clarity, predictable runtime behavior, and stability** over rapid feature expansion.

```cpp
#include <iostream>
#include <wirestead/wirestead.hpp>

auto client = wirestead::tcp_client("127.0.0.1", 8080)
    .max_retries(3)
    .on_data([](const wirestead::MessageContext& ctx) {
        std::cout << "received " << ctx.data().size() << " bytes\n";
    })
    .build();

client->start_sync();
client->send("hello");
```

The same shape builds a serial port, a UDP socket or a UDS endpoint — see [Quick Start](docs/quickstart.md).

> **Security note**: transports send data in plaintext by default. TCP can do TLS in a build configured with `-DWIRESTEAD_ENABLE_TLS=ON` - server and client, with the client verifying the server; UDP, Serial and UDS cannot, and DTLS is not supported. See [Security and Threat Model](https://github.com/wirestead/wirestead/blob/main/docs/security.md) before using `wirestead` over an untrusted network.

## How Wirestead compares

Wirestead is a **multi-transport async library**. Most alternatives are either a single-transport library or a set of ready-to-run ROS nodes, so the useful question is usually which shape you need rather than which has more features.

| | Transports | Async | Platforms | Install |
| --- | --- | --- | --- | --- |
| **Wirestead** | Serial, TCP, UDP, UDS | yes, one `io_context` model across all four | Linux, macOS, Windows — x64 and arm64 | vcpkg, FetchContent, PyPI |
| [transport_drivers](https://github.com/ros-drivers/transport_drivers) | Serial, UDP | yes (standalone Asio) | Linux (ROS 2) | rosdep / apt |
| [libserial](https://github.com/crayzeewulf/libserial) | Serial | no | Linux only | `apt install libserial-dev` |
| [serialib](https://github.com/imabot2/serialib) | Serial | no | Linux, Windows | copy two files |
| Boost.Asio directly | everything | yes | everywhere | you already have it |

Wirestead fits best when one application speaks over more than one transport — a serial sensor, a TCP command server, a UDP telemetry feed — and you would otherwise write reconnect, buffering and framing three times against three different APIs. The four transports share one API, so switching between them is a builder change rather than a rewrite. It runs on Linux, macOS and Windows alike, which the serial-only libraries above do not, and latency is published per release on real hardware: see [the benchmark releases](https://github.com/wirestead/wirestead-benchmarks/releases). The Feature Highlights below cover what it adds on top of Asio.

### When to use something else

- **You only need serial, on Linux.** `apt install libserial-dev` and you are done. Wirestead pulls in Boost and asks you to build it; that is a poor trade for one serial port.
- **You want the smallest possible dependency.** serialib is two files with no dependencies at all.
- **You are on ROS 2 and want a bridge, not a library.** `transport_drivers` ships `serial_bridge` and `udp_bridge_node_exe` — running executables that move bytes between a device and a topic. Wirestead gives you a library to write your own node against; `wirestead_ros` provides a lifecycle shutdown gate, `RuntimeStats` reporting onto `diagnostic_updater`, and a reference lifecycle driver, but no drop-in bridge node. If a bridge is all you need, `transport_drivers` is less work.
- **You know Asio well and want direct control.** Any wrapper is in your way. Wirestead is a wrapper.

## Feature Highlights

* **Unified transport surface**: Consistent builders and wrappers for TCP client/server, UDP, Serial, and UDS.
* **Callback-scoped data views**: Avoid unnecessary copies during callbacks, with explicit ownership-copy helpers for stored data. Each payload carries the time it arrived, so a timestamp does not have to be taken after the fact.
* **Message framing**: Line-delimited, start/end pattern, and length-prefixed framers, or your own `IFramer`.
* **Optional TLS**: TCP client and server in a build configured with `-DWIRESTEAD_ENABLE_TLS=ON`, with the client verifying the server.
* **Fluent API with CRTP Builders**: Type-safe configuration with improved method chaining.
* **Built for devices**: Serial low-latency mode and RS-485, UDP multicast, a per-channel silence age for spotting a sensor that stopped talking, and a hook for putting the io threads on a real-time policy. See [Tuning](docs/tuning.md).
* **Tested runtime behavior**: Unit, integration, and end-to-end test suites are part of the repository and documented in `test/`.

## Requirements

* **C++20 compiler**: GCC 10+, Clang 14+, or MSVC 2022. CMake enforces these and fails the configure step below them. CI builds GCC on Ubuntu 22.04 and 24.04, Clang on Ubuntu 24.04 and macOS, and MSVC on Windows, each on x64 and arm64.
* CMake 3.12 or later for plain builds; CMake 3.21 or later for the repository presets
* Boost 1.74.0 or later, which covers the system packages on Ubuntu 22.04 (1.74), RHEL 9 (1.75) and Ubuntu 24.04 (1.83). vcpkg remains the recommended dependency supplier; CI builds against the 1.74 floor as well as current Boost.

## 📦 Installation

### vcpkg (recommended)

```bash
vcpkg install wirestead
```

### CMake FetchContent

```cmake
include(FetchContent)
FetchContent_Declare(wirestead
    GIT_REPOSITORY https://github.com/wirestead/wirestead.git
    GIT_TAG v0.9.6)
FetchContent_MakeAvailable(wirestead)
target_link_libraries(your_target PRIVATE wirestead::wirestead)
```

### Python

```bash
pip install wirestead
```

See [Installation](./docs/installation.md) for a `find_package` consumer, a source build, and the full option list.

### Contributor development setup

```bash
./scripts/setup_dev_env.sh
cmake --preset dev-linux-x64
cmake --build --preset dev-linux-x64
```

The setup script installs Boost and spdlog through an untracked, repository-local `vcpkg/` checkout by default. Delete that directory any time to reclaim space; rerun the setup script to recreate it. Set `VCPKG_ROOT` before running the script if you want to reuse an external vcpkg checkout.
CMake remains the version gate and rejects Boost versions older than 1.74.0.
The preset-based contributor workflow uses `CMakePresets.json` schema version 3, so those `cmake --preset ...` commands require CMake 3.21+.

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the full contributor workflow: running tests, `scripts/verify.sh`, commit conventions, and PR expectations.

## Coming from Unilink

Wirestead is the successor to Unilink. The `unilink` compatibility aliases were removed in v0.10.0; v0.9.x is the last line that installs them. See [Migrating from Unilink](./docs/migration-from-unilink.md).

## 📚 Documentation

Core repository entrypoints:

- [Quick Start](https://github.com/wirestead/wirestead/blob/main/docs/quickstart.md)
- [Installation](https://github.com/wirestead/wirestead/blob/main/docs/installation.md)
- [API Stability Summary](https://github.com/wirestead/wirestead/blob/main/docs/api_stability.md)
- [Unilink Migration Guide](https://github.com/wirestead/wirestead/blob/main/docs/migration-from-unilink.md)
- [Changelog](https://github.com/wirestead/wirestead/blob/main/CHANGELOG.md)
- [Error Model](https://github.com/wirestead/wirestead/blob/main/docs/error_model.md)
- [Security and Threat Model](https://github.com/wirestead/wirestead/blob/main/docs/security.md)
- [Callback Data Lifetime](https://github.com/wirestead/wirestead/blob/main/docs/callbacks.md)
- [Performance Validation](https://github.com/wirestead/wirestead/blob/main/docs/performance_validation.md)
- [Release Checklist](https://github.com/wirestead/wirestead/blob/main/docs/release_checklist.md)

Useful external repositories:

* [Documentation](https://github.com/wirestead/wirestead-docs) ([published site](https://wirestead.github.io/wirestead-docs/))
* [Python bindings](https://github.com/wirestead/wirestead-python)
* [Examples](https://github.com/wirestead/wirestead-examples)
* [Containers](https://github.com/wirestead/wirestead-container)

---

## 📄 License

**Wirestead** is released under the Apache License, Version 2.0.

Commercial use, modification, and redistribution are permitted.
For details, see the [LICENSE](./LICENSE) and [NOTICE](./NOTICE) files.

Copyright © 2025 Jinwoo Sung
