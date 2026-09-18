# API Stability Summary

## Summary

`wirestead` is currently pre-1.0.

The project tries to keep documented user-facing behavior stable within a minor
line, but C++ API and ABI compatibility are not frozen before v1.0.

## Stable user-facing surface

Prefer:

```cpp
#include <wirestead/wirestead.hpp>
```

User-facing APIs include:

- builders and wrappers
- `RuntimeStats`
- `MessageContext`, `ConnectionContext`, `ErrorContext`
- `send(...)`, `try_send(...)`
- `send_move(...)`, `try_send_move(...)`
- `send_shared(...)`, `try_send_shared(...)`
- TCP/UDP socket tuning builder options
- TCP/UDP idle timeout builder options

## Supported but pre-1.0 unstable APIs

These APIs are available, but may still change before v1.0:

- framer APIs
- config APIs
- diagnostics APIs

## Builder return convention

Every builder setter returns `Derived&` and mutates the builder in place, so a
chain and a sequence of separate statements do the same thing:

```cpp
auto b = wirestead::tcp_client("127.0.0.1", 8080);
b.on_data(handler);          // b is still usable
auto client = b.on_error(eh).build();
```

Before v0.9.3 the handler setters instead returned a new builder of a different
type and left the original moved-from, while the other setters returned a
reference. That split is gone, along with the `BuilderState` template parameter
and the `Rebind` alias. See the CHANGELOG for what to change; chained code,
which is how every example is written, needs nothing.

## ABI

C++ ABI stability is not guaranteed before v1.0. The v0.9.0 Wirestead rename
changes C++ symbols, library filenames, and SONAMEs; consumers moving from
v0.8.x must rebuild.

Patch releases may require relinking and retesting. Applications should not
mix headers from one minor line with libraries from another.

## Release Compatibility Policy

Patch releases should avoid:

- removing public include paths
- removing public APIs
- changing existing default behavior
- removing compatibility shims
- reducing supported platforms without prior notice

Patch releases may include bug fixes, packaging fixes, CI fixes,
documentation updates, test additions, and compatible implementation changes.

Minor releases before v1.0 may introduce API changes when needed, but breaking
changes should be documented in `CHANGELOG.md` and migration notes.

Deprecated APIs and compatibility aliases should remain for at least the current
minor line unless a security or correctness issue makes that impossible. The
Unilink compatibility aliases were kept for the whole v0.9.x line and removed
in v0.10.0; see `docs/migration-from-unilink.md`.

## Diagnostics

`RuntimeStats` is a user-facing diagnostics snapshot. It is not a
synchronization primitive.

## Design-only APIs

`SendResult` is currently design-only unless explicitly implemented in a future
release. It is not part of the runtime API in the current release line.

## Exported symbols

The shared library exports the symbols this project owns and nothing else.
Hidden visibility alone does not achieve that, because weak and unique symbols
survive it: before this was enforced, `libwirestead.so` also re-exported
`boost::asio::detail` internals it merely linked against. A consumer loading a
different Boost into the same process could have those merged, which is an ODR
violation that shows up as runtime misbehavior rather than a link error.

This restricts what leaks out, not what Wirestead offers. Every `wirestead`
symbol stays exported, including the typeinfo and vtables that exceptions
thrown across the library boundary need in order to be caught by type. The
lists live in `cmake/wirestead.map` (ELF) and `cmake/wirestead.exp` (Mach-O);
MSVC needs no equivalent because exports there are already opt-in through
`__declspec(dllexport)`. Set `WIRESTEAD_LIMIT_EXPORTED_SYMBOLS=OFF` to fall
back to the previous behavior.

## Internal headers

Internal headers are not part of the compatibility guarantee before v1.0.
Prefer the public facade and documented wrapper/builder APIs for application
code.

Compatibility is not guaranteed for deep includes under:

- `transport/*`
- `interface/*`
- `memory/*`
- `concurrency/*`
- `factory/*`

A further consequence applies to the shared library. Out-of-line functions
declared in those headers are not always exported from it, because they carry
no export annotation and hidden visibility keeps them internal. Code that
includes an internal header and calls such a function links against the static
library but not the shared one. The documented public surface is unaffected;
it is exported from both.

These headers are installed for implementation and advanced integration needs,
but application code should include `<wirestead/wirestead.hpp>` unless a documented
advanced API requires otherwise.

## Python Binding Compatibility

The Python binding follows the C++ core minor release line. Python package
0.9.x targets Wirestead C++ core 0.9.x. Patch versions may differ when the
Python package contains packaging, CI, binding, or documentation fixes that do
not require a matching C++ core patch release.
