# Migrating from UniLink to Wirestead

Wirestead is the canonical project, package, build, and C++ API identity
starting with v0.9.0. UniLink names are kept only as a v0.9.x source and build
compatibility layer for existing consumers.

The rename changes C++ mangled symbols, library filenames, and shared library
SONAMEs. Existing v0.8.x binaries are not ABI-compatible with v0.9.0 and must
be rebuilt.

## Compatibility Table

| Area | Wirestead canonical surface | UniLink compatibility surface |
|---|---|---|
| C++ namespace | `namespace wirestead` | `namespace unilink = wirestead` |
| Base exception | `wirestead::diagnostics::WiresteadException` | `wirestead::diagnostics::UnilinkException` type alias |
| Header path | `<wirestead/wirestead.hpp>` and `<wirestead/...>` | `<unilink/...>` forwarding headers |
| CMake project | `project(wirestead)` | legacy cache variables documented here |
| CMake package | `find_package(wirestead CONFIG REQUIRED)` | `find_package(unilink CONFIG REQUIRED)` loads Wirestead and adds legacy targets |
| CMake target | `wirestead::wirestead` | `unilink::unilink` links to `wirestead::wirestead` |
| CMake options | `WIRESTEAD_*` | `UNILINK_*` inputs are forwarded during configure |
| Environment variable | `WIRESTEAD_LOG_LEVEL` | `UNILINK_LOG_LEVEL` fallback when the Wirestead variable is absent |
| Export macro | `WIRESTEAD_API` | `UNILINK_API` alias |
| pkg-config | `wirestead.pc` with `-lwirestead` | `unilink.pc` with `-lwirestead` |
| Library file | `libwirestead` | no `libunilink` binary compatibility shim |
| SONAME | `libwirestead.so.0` on Linux | no `libunilink` SONAME |
| Release assets | `wirestead-<version>-*` | old `unilink-<version>-*` asset names are not produced |
| vcpkg port | planned `wirestead` port after v0.9.0 | existing `jwsung91-unilink` port remains the legacy/current route until deprecated |

## Source Compatibility

Existing documented source usage continues to compile when rebuilt against
v0.9.x:

```cpp
#include <unilink/unilink.hpp>

unilink::builder::TcpClientBuilderDefault builder("127.0.0.1", 8080);
```

The compatibility layer is intentionally narrow. It does not support reopening
`namespace unilink { ... }`, direct forward declarations of internal UniLink
symbols, undocumented internal headers, or checks that hard-code mangled symbol
names or old shared library filenames.

## ABI Compatibility

v0.9.0 is an ABI break from v0.8.x. Consumers must rebuild all binaries and
libraries that link to Wirestead. A `libunilink` symlink is intentionally not
provided because the C++ symbols now live under `wirestead::`; a filename-only
shim would hide the ABI break without making old binaries work.

## Build Compatibility

`WIRESTEAD_*` CMake options are canonical. `UNILINK_*` options are accepted as
v0.9.x compatibility inputs and forwarded to the matching `WIRESTEAD_*` option
early in configure. If both names are explicitly set to the same value, configure
continues. If both names are explicitly set to different values, configure fails
with `FATAL_ERROR`.

`WIRESTEAD_LOG_LEVEL` is read first at runtime. `UNILINK_LOG_LEVEL` is read only
when `WIRESTEAD_LOG_LEVEL` is unset or empty. When both are set, Wirestead wins
and the logger emits a diagnostic line so the precedence is visible.

## Migration Examples

| Old | New |
|---|---|
| `find_package(unilink CONFIG REQUIRED)` | `find_package(wirestead CONFIG REQUIRED)` |
| `target_link_libraries(app PRIVATE unilink::unilink)` | `target_link_libraries(app PRIVATE wirestead::wirestead)` |
| `#include <unilink/unilink.hpp>` | `#include <wirestead/wirestead.hpp>` |
| `unilink::tcp_client(...)` | `wirestead::tcp_client(...)` |
| `catch (const unilink::diagnostics::UnilinkException&)` | `catch (const wirestead::diagnostics::WiresteadException&)` |
| `-DUNILINK_ENABLE_CONFIG=ON` | `-DWIRESTEAD_ENABLE_CONFIG=ON` |
| `UNILINK_LOG_LEVEL=DEBUG` | `WIRESTEAD_LOG_LEVEL=DEBUG` |
| `pkg-config --libs unilink` | `pkg-config --libs wirestead` |

## vcpkg Status

The canonical vcpkg port will be `wirestead` after a validated v0.9.0 release is
available. Until that PR lands, existing vcpkg instructions that reference
`jwsung91-unilink` describe the legacy/current port. That port is expected to
become a deprecated compatibility port depending on `wirestead`.

## External Repositories

External documentation, Python binding, example, benchmark, and container
repositories are outside this source-tree rename. Those repositories already
moved from the `unilink-lab` organization to `wirestead`, and have since been
renamed to `wirestead-docs`, `wirestead-python`, `wirestead-examples`,
`wirestead-benchmarks`, and `wirestead-container` respectively.

## Version Policy

The UniLink compatibility layer is guaranteed for the v0.9.x line. Its removal
version is not fixed; removal will be decided later from real usage data and
will not make UniLink the canonical identity again.
