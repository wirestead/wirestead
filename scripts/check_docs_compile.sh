#!/bin/bash
# Compiles every documented C++ sample against this checkout's headers.
#
# check_docs_coverage.sh answers "was newly added API documented". It cannot
# answer "does the documentation still build", because removing or renaming an
# API leaves prose that still reads fine and code that no longer compiles. #639
# removed AsyncLogConfig::batch_size and left three snippets in wirestead-docs
# that break the moment the next tag ships; a sweep found eighteen more that
# were already broken, some dating to the unilink rename. Nothing caught any of
# them, so this answers the question mechanically.
#
#   scripts/check_docs_compile.sh [path-to-wirestead-docs]
#
# With no docs path it does a shallow clone into a temporary directory. Both
# this repository's docs/ and the site are checked.
#
# Most samples are fragments, not programs, so most of them fail to compile for
# reasons that say nothing about the API: an undeclared `client`, a stray
# markdown line, a mock type that only exists in the prose around it. Only the
# errors that mean a name moved are reported. That still leaves false
# positives - read the list, do not obey it.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

DOCS_PATH="${1:-}"

CXX="${CXX:-g++}"
if ! command -v "$CXX" >/dev/null 2>&1; then
  echo "No C++ compiler found (tried '$CXX'; set CXX to override)." >&2
  exit 2
fi

CLEANUP_DIR="$(mktemp -d)"
trap 'rm -rf "$CLEANUP_DIR"' EXIT

if [ -z "$DOCS_PATH" ]; then
  echo "Cloning wirestead-docs..." >&2
  git clone --depth 1 --quiet https://github.com/wirestead/wirestead-docs.git \
    "$CLEANUP_DIR/docs" 2>/dev/null || {
      echo "Could not clone wirestead-docs; pass a local checkout as the first argument." >&2
      exit 2
    }
  DOCS_PATH="$CLEANUP_DIR/docs"
fi

if [ ! -d "$DOCS_PATH" ]; then
  echo "Not a directory: $DOCS_PATH" >&2
  exit 2
fi

# Headers a sample may name without including. wirestead.hpp is the umbrella a
# reader includes; the rest are the public corners it deliberately leaves out
# (pools, tracking, thread-safe state, the config structs and the factory).
# A sample's own #include lines are hoisted above this preamble, so a block that
# includes what it uses still compiles the way its reader would build it.
PREAMBLE="$CLEANUP_DIR/preamble.h"
cat > "$PREAMBLE" <<'EOF'
#include "wirestead/wirestead.hpp"
#include "wirestead/base/common.hpp"
#include "wirestead/base/constants.hpp"
#include "wirestead/concurrency/io_context_manager.hpp"
#include "wirestead/concurrency/thread_safe_state.hpp"
#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/factory/channel_factory.hpp"
#include "wirestead/interface/channel.hpp"
#include "wirestead/memory/memory_pool.hpp"
#include "wirestead/memory/memory_tracker.hpp"
#include "wirestead/memory/safe_data_buffer.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/transport/base/reconnect_policy.hpp"
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <any>
#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
using namespace wirestead;
using namespace std::chrono_literals;
EOF

mapfile -t FILES < <(
  { find docs README.md -name '*.md' -type f 2>/dev/null
    find "$DOCS_PATH" -name '*.md' -type f -not -path '*/.*' 2>/dev/null
  } | sort -u
)

blocks=0
reported=0
FINDINGS="$CLEANUP_DIR/findings"
: > "$FINDINGS"

for md in "${FILES[@]}"; do
  # One file per fenced cpp block, named so the line number survives into the
  # report. Fence info strings other than cpp/c++/cc are skipped: a bash or ini
  # block is not a compilation unit.
  rm -rf "$CLEANUP_DIR/blocks"
  mkdir -p "$CLEANUP_DIR/blocks"
  awk -v out="$CLEANUP_DIR/blocks" '
    /^```(cpp|c\+\+|cc)[[:space:]]*$/ && !inblock { inblock=1; start=NR+1; f=out "/" start ".cc"; next }
    /^```[[:space:]]*$/ && inblock { inblock=0; close(f); next }
    inblock { print > f }
  ' "$md"

  for block in "$CLEANUP_DIR/blocks"/*.cc; do
    [ -e "$block" ] || continue
    blocks=$((blocks + 1))
    line="$(basename "$block" .cc)"
    tu="$CLEANUP_DIR/tu.cc"

    # A sample's #include lines move above the wrapper. Left inside the
    # generated function body they are still legal preprocessor input but every
    # declaration they bring lands at block scope, which turns one missing
    # header into a cascade of "is not a member of" that looks exactly like the
    # drift being hunted.
    grep -E '^[[:space:]]*#include' "$block" > "$tu" || true
    cat "$PREAMBLE" >> "$tu"
    if grep -qE '\bint[[:space:]]+main[[:space:]]*\(' "$block"; then
      grep -vE '^[[:space:]]*#include' "$block" >> "$tu" || true
    else
      echo 'void wirestead_doc_block_() {' >> "$tu"
      grep -vE '^[[:space:]]*#include' "$block" >> "$tu" || true
      echo '}' >> "$tu"
    fi

    err="$("$CXX" -fsyntax-only -std=c++20 -I. -DWIRESTEAD_ENABLE_CONFIG=1 \
             -w -fpermissive "$tu" 2>&1)" && continue

    # The error classes that mean a name moved. Everything else - undeclared
    # identifiers, stray prose, incomplete mock types - is what a fragment
    # looks like when it is lifted out of the paragraph explaining it.
    hits="$(printf '%s\n' "$err" | grep -E \
      'has no member named|is not a member of|no matching function for call|too many arguments to function|no declaration matches' |
      sed 's|^[^:]*:[0-9]*:[0-9]*: ||' | sort -u | head -3 || true)"
    [ -z "$hits" ] && continue

    reported=$((reported + 1))
    {
      echo "${md}:${line}"
      printf '  %s\n' "$hits"
    } >> "$FINDINGS"
  done
done

echo "Documented C++ blocks compiled: $blocks"
echo "Blocks whose errors look like an API moved: $reported"

if [ "$reported" -eq 0 ]; then
  echo
  echo "Nothing to fix."
  exit 0
fi

echo
cat "$FINDINGS"
echo
echo "Each of these is either documentation that no longer matches the headers"
echo "or a fragment the extraction could not stand up on its own. Check them"
echo "individually; a sample that was always illustrative needs no change."
exit 1
