# Wirestead Core Documentation

## What lives here, and what lives on the site

Wirestead's documentation is split across two repositories, and the split is by
**question**, not by topic:

| | this directory | [wirestead-docs](https://github.com/wirestead/wirestead-docs) |
| --- | --- | --- |
| answers | *how does this behave, and why* | *how do I use this* |
| examples | the callback data lifetime, the error model, the threat model, what a tuning knob costs | quick start, installation, the API guide, the transport matrix, troubleshooting |
| reader | someone who has to reason about behaviour, and contributors | someone building an application |

Three documents deliberately exist in both: `quickstart.md`,
`installation.md` and `api_stability.md`. The release checklist requires this
repository to carry them, so a reader who arrives at the source has something
to stand on. The versions here are **minimal on purpose**; the site's are the
extended ones, and neither should grow into the other.

API signatures are in neither. Doxygen generates those from these headers, so
they cannot go stale, and prose that repeats a signature only creates something
to keep in sync.

When a public API changes, `scripts/check_docs_coverage.sh` lists what has no
mention on either side. It exists because two releases shipped eight APIs that
neither had.

`scripts/check_docs_compile.sh` covers the other direction: it compiles every
documented C++ sample, on both sides, against these headers. Prose keeps
reading fine after an API is removed or renamed, so nothing else notices.

## Core entrypoints

- [Quick Start](quickstart.md)
- [Installation](installation.md)
- [Callback Data Lifetime](callbacks.md)
- [Error Model](error_model.md)
- [Security and Threat Model](security.md)
- [API Stability Summary](api_stability.md)
- [Tuning](tuning.md)
- [Performance Validation](performance_validation.md)
- [ROS 2 Support Analysis](ros2_support_analysis.md)
- [Migrating from Unilink](migration-from-unilink.md)
- [Release Checklist](release_checklist.md)

## Extended user documentation

Quick start tutorials, the full API guide, the transport feature matrix,
troubleshooting and platform requirements live on the site:
<https://wirestead.github.io/wirestead-docs/>
