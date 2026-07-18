# Migrating from UniLink to Wirestead

`unilink` (the project, the repository, and the brand) is being renamed to
Wirestead. This document is the compatibility contract for that transition:
what changes, what doesn't, when, and why. It exists so that no naming
decision has to be made ad hoc inside an implementation PR — see
[Decisions](#decisions) below for the ones that were open questions before
this document was written.

Full policy for the project in general (of which this is a part) is
maintained in the [API Stability Summary](./api_stability.md). The short
version that matters most for this migration: **`unilink`/Wirestead is
currently pre-1.0, and C++ ABI stability is not guaranteed before v1.0.**
That significantly lowers the risk of the binary-identity changes discussed
below, since consumers linking against a pre-1.0 release have never had an
ABI stability guarantee to begin with.

## Compatibility table

| Area | New name | Old name handling |
|---|---|---|
| CMake project | `wirestead` | Internal `project()` name change is deferred to the v0.9.x implementation PR; does not affect consumers. |
| CMake package | `find_package(wirestead)` | `find_package(unilink)` continues to work, unchanged, indefinitely (no fixed removal version yet — see [Decisions](#decisions)). |
| CMake target | `wirestead::wirestead` | `unilink::unilink` kept as an `ALIAS` (or equivalent) target; not marked deprecated in v0.9.x. |
| Header path | `<wirestead/...>` | `<unilink/...>` headers kept; new headers are either the canonical copy with `<wirestead/...>` as a forwarding shim, or vice versa (implementation PR's call — no consumer-visible difference either way). |
| C++ namespace | `wirestead` (not introduced) | `namespace unilink` is **not renamed**. See [Decisions](#decisions) — this is the one row where the recommendation is "don't do the rename" rather than "do it with a compat shim." |
| CMake options | `WIRESTEAD_*` | `UNILINK_*` values are read and forwarded to the corresponding `WIRESTEAD_*` option; sets any consumer using only the old name up correctly, no silent no-ops (see [Decisions](#decisions) for the conflict rule). |
| Environment variable | `WIRESTEAD_LOG_LEVEL` | `UNILINK_LOG_LEVEL` still read. If both are set, `WIRESTEAD_LOG_LEVEL` wins (see [Decisions](#decisions)). |
| Export macro | `WIRESTEAD_API` | `UNILINK_API` kept as the macro every public symbol is actually annotated with; `WIRESTEAD_API` is defined as an alias of it, not the other way around, so there's exactly one export-visibility decision to keep correct. |
| Exception types | not introduced | `UnilinkException` and its subclasses (`BuilderException`, `ValidationException`, `MemoryException`, `ConnectionException`, `ConfigurationException`) are **not renamed or aliased**. Same reasoning as the namespace row. |
| pkg-config | `wirestead.pc` | `unilink.pc` kept and installed alongside it. |
| Library file | `libwirestead` | `libunilink` is the only binary name through v0.9.x and the v1.0.0 line (see [Decisions](#decisions)). |
| SONAME | Wirestead-based | No change before v1.0.0; ABI is unversioned/unstable pre-1.0 regardless (see [API Stability Summary](./api_stability.md)). |
| vcpkg port | `wirestead` | `unilink` becomes a deprecated compatibility port once `wirestead` has a v0.9.0 release to point at (Stage 6 of the migration plan; not started by this document). |

## Decisions

These were the open questions the migration plan flagged as needing an
answer before any implementation PR. Answers below; each is revisitable, but
should be treated as the default rather than re-litigated per-PR.

**End-of-support version for the old (`unilink`) names.** Not fixed yet, and
deliberately so — picking a removal version now would be guessing at
real-world `unilink`/`jwsung91-unilink` usage the project doesn't have
data on. What *is* fixed: the old names are guaranteed to keep working
through v0.9.x and the entire v1.0.x line. Any removal is a major-version
(v2.0+) decision, made after there's actual vcpkg/download usage data to
look at, not on a fixed calendar.

**Version where deprecation warnings start.** v1.0.0 at the earliest — not
v0.9.x. The point of v0.9.x is to give existing UniLink consumers a fully
quiet, warning-free bridge release; introducing deprecation noise in the
same release that introduces the new name would undercut that. Revisit
whether v1.0.0 is still the right point once v0.9.x usage is visible.

**Priority when both `UNILINK_*` and `WIRESTEAD_*` are set.**
- CMake options: if both are explicitly set (not just defaulted) to
  *different* values, `cmake` should fail with `FATAL_ERROR` naming both
  values and which option won, rather than silently picking one. Build-time
  configuration is exactly the kind of thing that should never be
  ambiguous. If they're set to the *same* value, no error (that's just a
  redundant-but-consistent configuration).
- `WIRESTEAD_LOG_LEVEL` / `UNILINK_LOG_LEVEL`: this one is a runtime
  environment variable, not a build-time input, and silently picking a
  well-defined precedence is normal for env vars (compare `PY``THONPATH`
  vs. tool-specific overrides elsewhere in the ecosystem). Rule: if both are
  set, `WIRESTEAD_LOG_LEVEL` wins and a one-line log message on startup says
  so, so it's discoverable rather than silent.

**Whether to actually rename `namespace unilink` and the `*Exception`
types.** No. Recommendation is to leave both alone, not to rename-with-alias
them. Reasoning:
- Every other row in the compatibility table (package name, target name,
  header path, CMake option prefix, env var, export macro, pkg-config,
  binary name) is a *packaging/build-system* identity, invisible inside a
  consumer's C++ source. A namespace or exception-hierarchy rename is not —
  it's the one change that would force every existing consumer to touch
  their `.cpp` files (`unilink::` qualifications, `catch (unilink::...)`
  clauses) even with a compatibility alias in place, because C++ namespace
  aliases and exception-type aliases don't fully paper over the difference
  the way a CMake `ALIAS` target or a forwarding header does.
- The migration plan's own ground rules (§3) already exclude `namespace
  unilink` from Stage 1, and nothing in the stated goals of this rename
  (repository/brand identity, vcpkg discoverability) requires touching the
  C++ symbol surface to achieve them.
- If this is revisited later, it should be its own explicitly-scoped
  proposal, not a default carried by this document.

**When the binary name (`libunilink` → `libwirestead`) and SONAME change.**
Not before v1.0.0, and even at v1.0.0 it needs its own decision (this
document doesn't pre-approve it, it just sets the earliest point it could
happen). Rationale: renaming the binary breaks every existing dynamically-
linked consumer's runtime loader path, which is a much sharper break than
anything else in this table — worth doing at most once, at a deliberate
major-version boundary, not as a side effect of the brand rename.

**Source / build / ABI compatibility, stated explicitly:**
- **Source compatibility**: full, indefinitely, for any consumer using the
  `unilink::` namespace and `<unilink/...>` headers. No source changes are
  required to keep building against v0.9.x or the v1.0.x line.
- **Build compatibility**: full for v0.9.x. `find_package(unilink)`,
  `UNILINK_*` options, and `unilink.pc` all keep working unmodified.
- **ABI compatibility**: not guaranteed, same as it has never been
  guaranteed pre-1.0 (see [API Stability Summary](./api_stability.md)).
  This migration doesn't change that policy in either direction — it's
  called out here only so it isn't confused with the source/build
  guarantees above, which *are* new commitments made by this document.

## For existing UniLink users

Nothing is required. `find_package(unilink)`, `<unilink/...>`, `namespace
unilink`, `UNILINK_*` options, `UNILINK_LOG_LEVEL`, and `libunilink` all
continue to work unchanged in v0.9.x. Switching to the `wirestead` names is
optional and cosmetic (package/target/header spelling only) for as long as
this document's compatibility table holds.

For new integrations, prefer the `wirestead` spelling
(`find_package(wirestead)`, `wirestead::wirestead`, `<wirestead/...>`,
`WIRESTEAD_*` options) — it's the name the project is going by now, and the
one vcpkg's `wirestead` port (once published) will install under.

| Old | New |
|---|---|
| `vcpkg install jwsung91-unilink` | `vcpkg install wirestead` (once the vcpkg port lands; see Stage 6 of the migration plan) |
| `find_package(unilink CONFIG REQUIRED)` | `find_package(wirestead CONFIG REQUIRED)` |
| `target_link_libraries(app PRIVATE unilink::unilink)` | `target_link_libraries(app PRIVATE wirestead::wirestead)` |
| `#include <unilink/unilink.hpp>` | `#include <wirestead/unilink.hpp>` |
| `-DUNILINK_ENABLE_CONFIG=ON` | `-DWIRESTEAD_ENABLE_CONFIG=ON` |
| `UNILINK_LOG_LEVEL=DEBUG` | `WIRESTEAD_LOG_LEVEL=DEBUG` |

C++ code itself is unaffected either way: `unilink::` remains the only
namespace, so `#include <wirestead/unilink.hpp>` still gives you
`unilink::builder::TcpClientBuilder`, not a `wirestead::` equivalent.

## Version policy

- **v0.9.x**: Wirestead names introduced across CMake package/target,
  headers, options, env var, export macro, and pkg-config. UniLink names
  keep working, with no deprecation warnings.
- **v1.0.0**: Binary name and SONAME change re-evaluated (not pre-approved
  by this document — a separate decision). Earliest point deprecation
  warnings could start on the old names, per [Decisions](#decisions).
- **Old-name removal**: no fixed version. Decided later, based on observed
  `unilink`/`jwsung91-unilink` usage, not on a calendar.

This document is the Stage 3 deliverable of the UniLink → Wirestead
migration plan; Stage 4 (`build: introduce Wirestead package names with
UniLink compatibility`, `refactor: add Wirestead headers, options and
public API aliases`) implements the table above.
