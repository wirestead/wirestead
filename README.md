![Wirestead](assets/wirestead-logo-horizontal.svg#gh-light-mode-only)
![Wirestead](assets/wirestead-logo-dark.svg#gh-dark-mode-only)

# Wirestead™

_Wirestead is the successor to UniLink._

**Robust, simple async communication for modern C++20.**

Serial · TCP · UDP · UDS

![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows%20%7C%20macOS-informational)
![vcpkg](https://img.shields.io/badge/vcpkg-jwsung91--unilink%20legacy/current-0078D6)
[![Coverage](https://img.shields.io/endpoint?url=https://wirestead.github.io/wirestead/coverage/badges/coverage.json)](https://wirestead.github.io/wirestead/coverage/)

## Description

Simple async C++ communication library for Serial, TCP, UDP, and Unix Domain Sockets

`wirestead` provides a unified interface for asynchronous communication across different transports, allowing applications to switch between Serial, TCP, UDP, and UDS with minimal code changes. The public C++ API exposes builders and wrappers for all four transport families.

The project prioritizes **API clarity, predictable runtime behavior, and stability** over rapid feature expansion.

> **Security note**: all transports send data in plaintext - there is no built-in TLS/DTLS support. See [Security and Threat Model](https://github.com/wirestead/wirestead/blob/main/docs/security.md) before using `wirestead` over an untrusted network.

## Feature Highlights

* **Unified transport surface**: Consistent builders and wrappers for TCP client/server, UDP, Serial, and UDS.
* **Callback-scoped data views**: Avoid unnecessary copies during callbacks, with explicit ownership-copy helpers for stored data.
* **Fluent API with CRTP Builders**: Type-safe configuration with improved method chaining.
* **Tested runtime behavior**: Unit, integration, and end-to-end test suites are part of the repository and documented in `test/`.

## Requirements

* **C++20 compiler and standard library**: GCC 10+, recent Clang/libc++, or MSVC 2022 (required)
* CMake 3.12 or later for plain builds; CMake 3.21 or later for the repository presets
* Boost 1.83.0 or later. vcpkg is the recommended dependency supplier; OS package manager Boost versions are supported only when they meet this minimum.

## 📦 Installation

### vcpkg (recommended)

```bash
vcpkg install jwsung91-unilink
```

The `wirestead` vcpkg port is planned after the v0.9.0 release. Until then,
`jwsung91-unilink` is the legacy/current port.

External documentation, examples, Python bindings, and container repositories
are still in their legacy locations until they are moved.

### Contributor Development Setup

```bash
./scripts/setup_dev_env.sh
cmake --preset dev-linux-x64
cmake --build --preset dev-linux-x64
```

The setup script installs Boost and spdlog through an untracked, repository-local `vcpkg/` checkout by default. Delete that directory any time to reclaim space; rerun the setup script to recreate it. Set `VCPKG_ROOT` before running the script if you want to reuse an external vcpkg checkout.
CMake remains the version gate and rejects Boost versions older than 1.83.0.
The preset-based contributor workflow uses `CMakePresets.json` schema version 3, so those `cmake --preset ...` commands require CMake 3.21+.

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the full contributor workflow: running tests, `scripts/verify.sh`, commit conventions, and PR expectations.

## 📚 Documentation

Core repository entrypoints:

- [Quick Start](https://github.com/wirestead/wirestead/blob/main/docs/quickstart.md)
- [Installation](https://github.com/wirestead/wirestead/blob/main/docs/installation.md)
- [API Stability Summary](https://github.com/wirestead/wirestead/blob/main/docs/api_stability.md)
- [Error Model](https://github.com/wirestead/wirestead/blob/main/docs/error_model.md)
- [Security and Threat Model](https://github.com/wirestead/wirestead/blob/main/docs/security.md)
- [Callback Data Lifetime](https://github.com/wirestead/wirestead/blob/main/docs/callbacks.md)
- [Performance Validation](https://github.com/wirestead/wirestead/blob/main/docs/performance_validation.md)
- [Release Checklist](https://github.com/wirestead/wirestead/blob/main/docs/release_checklist.md)

Useful external repositories:

* [Python bindings](https://github.com/wirestead/wirestead-python) (legacy/current)
* [Examples](https://github.com/wirestead/wirestead-examples) (legacy/current)
* [Containers](https://github.com/wirestead/wirestead-container) (legacy/current)

---

## 📄 License

**Wirestead** is released under the Apache License, Version 2.0.

Commercial use, modification, and redistribution are permitted.
For details, see the [LICENSE](./LICENSE) and [NOTICE](./NOTICE) files.

Copyright © 2025 Jinwoo Sung
