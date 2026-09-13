# Release Checklist

This checklist is maintained in the core repository because release packaging,
CI, CPack, and consumer smoke workflows live here.

## Version

- [ ] `CMakeLists.txt` project version is updated.
- [ ] `package.xml` version matches it. Bloom reads this file, not
      `CMakeLists.txt`, so the two drifting apart releases a Debian under
      the wrong version.
- [ ] `CHANGELOG.rst` has an entry for the release. The ROS release tooling
      reads this file; `CHANGELOG.md` alone is not enough.
- [ ] `packaging/wirestead.spec` carries the same version, and the same Boost
      and spdlog floors as `cmake/WiresteadDependencies.cmake`. RPM cannot read
      those from CMake, so nothing fails when they drift apart.
- [ ] Release tag matches the project version.
- [ ] README version-sensitive examples are still valid.

## Build and test

- [ ] Full CI passed.
- [ ] Unit tests passed.
- [ ] Integration tests passed.
- [ ] Installed consumer smoke passed for shared mode.
- [ ] Installed consumer smoke passed for static mode.

## Packaging

- [ ] `VCPKG_BASELINE` is current, and CI passed against that commit. The weekly
      `vcpkg baseline bump` workflow proposes this as a pull request; a bump is
      only good once CI is green on it, the Linux ARM64 jobs included.
- [ ] Release workflow dry-run completed.
- [ ] CPack package was generated.
- [ ] Package contains headers.
- [ ] Package contains library artifacts.
- [ ] Package contains `wiresteadConfig.cmake`.
- [ ] Package contains `wiresteadTargets.cmake`.
- [ ] Package contains `README.md`.
- [ ] Package contains `LICENSE`.
- [ ] Package contains `NOTICE`.

## Documentation

- [ ] Core README links to `wirestead-docs`.
- [ ] Core README and docs repo links were checked.
- [ ] `CHANGELOG.md` and migration guidance are current for compatibility
      changes.
- [ ] Core minimal docs are present:
  - [ ] `docs/installation.md`
  - [ ] `docs/quickstart.md`
  - [ ] `docs/api_stability.md`
  - [ ] `docs/release_checklist.md`
- [ ] `wirestead-docs` is updated when public API or behavior changes. Verified
      with `scripts/check_docs_coverage.sh <previous-tag>`, which lists public
      API added since that tag with no mention in either this repository's
      `docs/` or the documentation site. This box was tickable without checking
      before, and was ticked for two releases that added eight undocumented
      APIs.
- [ ] Documented code still compiles against the tag. Verified with
      `scripts/check_docs_compile.sh <path-to-wirestead-docs>`, which compiles
      every documented C++ sample in this repository's `docs/` and on the site
      and reports only the errors that mean a name moved.
      `check_docs_coverage.sh` cannot answer this: it finds API that was
      **added** and never documented, while API that was **removed or changed**
      leaves documentation that still reads fine and no longer builds. Removing
      `AsyncLogConfig::batch_size` in #639 left three documented snippets that
      break the moment this tag ships. Read the output rather than obeying it -
      a fragment that was always illustrative needs no change.
- [ ] Doxygen workflow in `wirestead-docs` passes.

## Benchmark / validation

- [ ] Latest relevant benchmark result is preserved in `wirestead-benchmarks`.
- [ ] Latest benchmark artifact or `wirestead-benchmarks` result is linked from the release notes.
- [ ] `.github/workflows/benchmark.yml` was run manually or by the latest nightly schedule.
- [ ] Contract-changing PRs link CI, Consumer Smoke, Benchmark, and TSAN run results in the PR or release notes.
- [ ] Installed consumer runtime smoke passed for the release candidate package.
- [ ] Optional TSAN workflow was reviewed or run for concurrency-sensitive changes.
- [ ] Benchmark result links were checked.
- [ ] Known benchmark/environment limitations are documented.
- [ ] UDP large-payload classification is current.
- [ ] Orin/WSL2 validation notes are current when relevant.

## External repositories

- [ ] `wirestead-python` compatibility impact checked.
- [ ] `wirestead-examples` compatibility impact checked, if examples depend on changed API.
- [ ] `wirestead-benchmarks` compatibility impact checked, if benchmark APIs changed.

## Package registries

These are downstream of the tag: they reference `vX.Y.Z`, so they can only be
updated once it is pushed. Both take the source tarball's SHA-256, which is the
GitHub archive (`/archive/refs/tags/vX.Y.Z.tar.gz`), not a release asset.

- [ ] `microsoft/vcpkg` port updated: `ports/wirestead/vcpkg.json` version and
      `portfile.cmake` `SHA512`. `REF` is `v${VERSION}`, so nothing else moves.
      Regenerate the version database with `./vcpkg x-add-version wirestead` —
      never edit `versions/` by hand.
- [ ] `conan-center-index` recipe updated: `recipes/wirestead/config.yml` and
      `recipes/wirestead/all/conandata.yml`. Conan Center wants a new recipe to
      carry **only** the latest version, so replace the version rather than
      appending to it.
- [ ] Conan recipe still matches the build system: every `WIRESTEAD_*` cache
      variable `conanfile.py` sets still exists, new options default to values
      the recipe can live with, and `test_package` compiles against the release
      API. The test package links the shared library, so a change to
      `cmake/wirestead.map` that narrows exports would break it.
- [ ] Conan recipe is not published yet — until conan-io/conan-center-index#30653
      is merged, updating it is a push to the `wirestead-recipe` fork branch and
      does not ship anything. CI there needs a maintainer to approve each run, so
      `Job scheduler: action_required` is the expected state, not a failure.
      Leave `docs/installation.md` free of Conan instructions until it merges.

## Release notes

- [ ] Release notes summarize user-facing changes.
- [ ] Breaking changes are explicitly marked.
- [ ] Known limitations are included.
- [ ] ABI stability disclaimer is included if pre-1.0.
