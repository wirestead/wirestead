#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

section() { echo -e "\n===== $* ====="; }

job_count() {
  if command -v nproc >/dev/null 2>&1; then
    nproc
  elif command -v sysctl >/dev/null 2>&1; then
    sysctl -n hw.ncpu
  else
    echo 2
  fi
}

JOBS="$(job_count)"
export JOBS

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
SKIP_FORMAT=0
SKIP_DOCS=0
ENABLE_TSAN=0

for arg in "$@"; do
  case "$arg" in
    --tests-only|-t)
      SKIP_FORMAT=1
      SKIP_DOCS=1
      ;;
    --skip-format)
      SKIP_FORMAT=1
      ;;
    --skip-docs)
      SKIP_DOCS=1
      ;;
    --tsan)
      ENABLE_TSAN=1
      ;;
    --help|-h)
      echo "Usage: $0 [options]"
      echo ""
      echo "Options:"
      echo "  --tests-only, -t   Skip formatting and doc snippets; run build + tests only"
      echo "  --skip-format      Skip clang-format / cmake-format step"
      echo "  --skip-docs        Accepted for compatibility; docs validation moved to wirestead-docs"
      echo "  --tsan             Enable ThreadSanitizer (TSAN) for compilation and contract testing (matches CI)"
      echo "  --help, -h         Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      exit 1
      ;;
  esac
done

WIRESTEAD_VERIFY_PRESET_USER_SET=1
if [[ -z "${WIRESTEAD_VERIFY_PRESET:-}" ]]; then
  WIRESTEAD_VERIFY_PRESET_USER_SET=0
  # Detect host platform and suggest a preset
  OS="$(uname -s)"
  ARCH="$(uname -m)"
  case "${OS}:${ARCH}" in
    Linux:x86_64)  WIRESTEAD_VERIFY_PRESET="dev-linux-x64" ;;
    Linux:aarch64 | Linux:arm64) WIRESTEAD_VERIFY_PRESET="dev-linux-arm64" ;;
    Darwin:arm64)  WIRESTEAD_VERIFY_PRESET="dev-macos-arm64" ;;
    Darwin:x86_64) WIRESTEAD_VERIFY_PRESET="dev-macos-x64" ;;
  esac
fi

WIRESTEAD_VERIFY_BUILD_DIR_USER_SET=1
if [[ -z "${WIRESTEAD_VERIFY_BUILD_DIR:-}" ]]; then
  WIRESTEAD_VERIFY_BUILD_DIR_USER_SET=0
  if [[ -n "${WIRESTEAD_VERIFY_PRESET:-}" ]]; then
    WIRESTEAD_VERIFY_BUILD_DIR="build/${WIRESTEAD_VERIFY_PRESET}"
  else
    WIRESTEAD_VERIFY_BUILD_DIR="build"
  fi
fi

if [[ -n "${WIRESTEAD_VERIFY_PRESET:-}" ]]; then
  VCPKG_TOOLCHAIN="${PROJECT_ROOT}/vcpkg/scripts/buildsystems/vcpkg.cmake"
  PRESET_UNAVAILABLE_REASON=""
  if [[ ! -f "${VCPKG_TOOLCHAIN}" ]]; then
    PRESET_UNAVAILABLE_REASON="missing local vcpkg toolchain: ${VCPKG_TOOLCHAIN}"
  elif ! command -v ninja >/dev/null 2>&1; then
    PRESET_UNAVAILABLE_REASON="missing Ninja build tool required by CMakePresets.json"
  fi

  if [[ -n "${PRESET_UNAVAILABLE_REASON}" ]]; then
    if [[ "${WIRESTEAD_VERIFY_PRESET_USER_SET}" -eq 1 ]]; then
      echo "Requested preset '${WIRESTEAD_VERIFY_PRESET}' is unavailable: ${PRESET_UNAVAILABLE_REASON}" >&2
      echo "Run ./scripts/setup_dev_env.sh or choose a non-preset build directory." >&2
      exit 1
    fi

    echo "Auto-detected preset '${WIRESTEAD_VERIFY_PRESET}' is unavailable: ${PRESET_UNAVAILABLE_REASON}"
    WIRESTEAD_VERIFY_PRESET=""
    if [[ "${WIRESTEAD_VERIFY_BUILD_DIR_USER_SET}" -eq 0 ]]; then
      WIRESTEAD_VERIFY_BUILD_DIR="build/verify"
    fi
    echo "Falling back to direct CMake configure in ${WIRESTEAD_VERIFY_BUILD_DIR}."
  fi
fi

# ---------------------------------------------------------------------------
# Step 1: Format
# ---------------------------------------------------------------------------
if [[ "${SKIP_FORMAT}" -eq 0 ]]; then
  section "Step 1: Formatting code"
  ./scripts/apply_clang_format.sh &
  CLANG_PID=$!
  ./scripts/apply_cmake_format.sh &
  CMAKE_PID=$!

  # Wait for both and check for failures
  wait "$CLANG_PID" || { echo "clang-format failed"; exit 1; }
  wait "$CMAKE_PID" || { echo "cmake-format failed"; exit 1; }
else
  section "Step 1: Formatting code [SKIPPED]"
fi

# ---------------------------------------------------------------------------
# Step 2: Build library
# ---------------------------------------------------------------------------
section "Step 2: Building project (Debug)"
EXTRA_CMAKE_ARGS=()
if [[ "${ENABLE_TSAN}" -eq 1 ]]; then
  EXTRA_CMAKE_ARGS+=("-DWIRESTEAD_ENABLE_SANITIZERS=ON" "-DWIRESTEAD_ENABLE_TSAN=ON" "-DWIRESTEAD_ENABLE_ASAN=OFF" "-DWIRESTEAD_ENABLE_UBSAN=OFF")
fi

if [[ -n "${WIRESTEAD_VERIFY_PRESET:-}" ]]; then
  echo "Using preset: ${WIRESTEAD_VERIFY_PRESET}"
  cmake --preset "${WIRESTEAD_VERIFY_PRESET}" "${EXTRA_CMAKE_ARGS[@]}"
else
  mkdir -p "${WIRESTEAD_VERIFY_BUILD_DIR}"
  # Check if local vcpkg toolchain exists and use it as fallback
  VCPKG_TOOLCHAIN="${PROJECT_ROOT}/vcpkg/scripts/buildsystems/vcpkg.cmake"
  if [[ -f "${VCPKG_TOOLCHAIN}" ]]; then
    echo "Using local vcpkg toolchain: ${VCPKG_TOOLCHAIN}"
    cmake -S . -B "${WIRESTEAD_VERIFY_BUILD_DIR}" \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_CXX_STANDARD=20 \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
      -DWIRESTEAD_BUILD_TESTS=ON \
      -DWIRESTEAD_ENABLE_CONFIG=ON \
      -DCMAKE_TOOLCHAIN_FILE="${VCPKG_TOOLCHAIN}" \
      "${EXTRA_CMAKE_ARGS[@]}"
  else
    cmake -S . -B "${WIRESTEAD_VERIFY_BUILD_DIR}" \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_CXX_STANDARD=20 \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
      -DWIRESTEAD_BUILD_TESTS=ON \
      -DWIRESTEAD_ENABLE_CONFIG=ON \
      "${EXTRA_CMAKE_ARGS[@]}"
  fi
fi

if [[ "${ENABLE_TSAN}" -eq 1 ]]; then
  setarch $(uname -m) -R cmake --build "${WIRESTEAD_VERIFY_BUILD_DIR}" -j"${JOBS}"
else
  cmake --build "${WIRESTEAD_VERIFY_BUILD_DIR}" -j"${JOBS}"
fi

# ---------------------------------------------------------------------------
# Step 3: Documentation snippets
# ---------------------------------------------------------------------------
section "Step 3: Documentation snippets moved to wirestead-docs"

# ---------------------------------------------------------------------------
# Step 4: Full test suite
# ---------------------------------------------------------------------------
section "Step 4: Running full test suite"
if [[ "${ENABLE_TSAN}" -eq 1 ]]; then
  export TSAN_OPTIONS="halt_on_error=1 second_deadlock_stack=1 history_size=7"
  # Run the focused TSAN tests that are run in CI to avoid known races in other components.
  FOCUSED_TESTS="StopContract|StopFromCallback|TcpClientLifecycle|TcpServerWrapperLifecycle|UdsClientWrapperLifecycle|UdsServerWrapperLifecycle|BackpressureStrategyTest|WrapperSendContractTest|ServerBroadcastContractTest"
  setarch $(uname -m) -R ctest --test-dir "${WIRESTEAD_VERIFY_BUILD_DIR}" -j"${JOBS}" --output-on-failure -R "${FOCUSED_TESTS}"
else
  ctest --test-dir "${WIRESTEAD_VERIFY_BUILD_DIR}" -j"${JOBS}" --output-on-failure
fi

echo ""
echo "===== [SUCCESS] All checks passed! ====="
