# Contributing to unilink

Thanks for your interest in contributing. This guide covers the human
contributor workflow: environment setup, local verification, commit/PR
conventions, and review expectations.

> AI coding agents working in this repository should follow `CLAUDE.md`
> (or `AGENTS.md` / `GEMINI.md`) instead - those files define the
> agent-specific rules and final-report format.

## Getting started

```bash
./scripts/setup_dev_env.sh
cmake --preset dev-linux-x64
cmake --build --preset dev-linux-x64
```

`setup_dev_env.sh` bootstraps a repository-local `vcpkg/` checkout and
installs Boost/spdlog through it. Delete `vcpkg/` any time to reclaim space;
rerun the script to recreate it. Set `VCPKG_ROOT` first if you want to reuse
an existing vcpkg installation.

`dev-linux-x64` is the recommended starting preset. See `CMakePresets.json`
for the full list of platform-specific presets
(`dev-linux-arm64`, `dev-macos-arm64`, `dev-macos-x64`, `dev-windows-x64`,
`release-linux-x64`). Presets require CMake 3.21+; a plain (non-preset)
build only needs CMake 3.12+.

## Running tests

See `test/README.md` for the full test layout (unit/integration/e2e) and the
CTest label taxonomy for running subsets. The short version:

```bash
cmake -S . -B build -DUNILINK_BUILD_TESTS=ON
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

Prefer `-j2` for build parallelism by default; drop to `-j1` on
memory-constrained environments (WSL, VMs, small CI runners).

## Verifying before you push

`./scripts/verify.sh` runs the same formatting, build, and test steps as CI.
Run it locally before opening a PR:

```bash
./scripts/verify.sh              # full check: format + build + tests
./scripts/verify.sh --tests-only # skip formatting, build + test only
./scripts/verify.sh --skip-format
./scripts/verify.sh --tsan       # enable ThreadSanitizer, matches the tsan CI job
```

Formatting is enforced by `.clang-format` and `.cmake-format.py`. Use
`scripts/apply_clang_format.sh` and `scripts/apply_cmake_format.sh` to fix
formatting automatically before committing.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>[optional scope]: <description>
```

Common types: `feat`, `fix`, `docs`, `test`, `refactor`, `style`, `perf`,
`build`, `ci`, `chore`. Use `!` after the type/scope or a `BREAKING CHANGE:`
footer for compatibility-breaking changes. Keep the subject concise,
lowercase, imperative mood, no trailing period.

## Opening a pull request

- Keep changes scoped to a single concern; avoid bundling unrelated
  refactors with a feature or fix.
- Fill out `.github/pull_request_template.md` (auto-populated when you open
  a PR): description, key changes, related issues, and the checklist
  (verify.sh run, tests updated, docs updated, style followed).
- Do not rename public APIs, files, or user-facing concepts unless the PR is
  explicitly about that change - see `docs/api_stability.md` for what is and
  isn't covered by the compatibility guarantee.
- Add or update tests for behavior changes. If you intentionally didn't,
  say why in the PR description.
- CI runs the full compile matrix (Linux/macOS/Windows/ARM), unit/
  integration/e2e suites, memory-safety jobs (ASan/UBSan/LSan), CodeQL, and a
  `code-quality` job that checks clang-format/cmake-format compliance. All
  of these must pass before merge.

## Where things live

- In-repo docs (`docs/`) cover repository-local topics: quickstart, error
  model, callback lifetime, API stability, security model. Full tutorials
  and runnable examples live in separate repositories:
  [unilink-docs](https://github.com/unilink-lab/unilink-docs) and
  [unilink-examples](https://github.com/unilink-lab/unilink-examples).
- Bug reports and feature requests: open a GitHub issue in this repository.
- Security issues: see `docs/security.md` before filing a public issue.
