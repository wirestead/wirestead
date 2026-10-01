/*
 * Copyright 2025 Jinwoo Sung
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <new>
#include <vector>

#include "wirestead/diagnostics/send_accounting.hpp"

// Replaces global operator new for this binary only, so it must stay its own
// executable and is excluded under TSan (see test/unit/CMakeLists.txt).
namespace {
thread_local bool t_fail_allocation = false;
}

void* operator new(std::size_t n) {
  if (t_fail_allocation) throw std::bad_alloc();
  if (void* p = std::malloc(n ? n : 1)) return p;
  throw std::bad_alloc();
}
void* operator new(std::size_t n, std::align_val_t al) {
  if (t_fail_allocation) throw std::bad_alloc();
  const auto align = static_cast<std::size_t>(al);
  if (void* p = std::aligned_alloc(align, (n + align - 1) / align * align)) return p;
  throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }

namespace {
using Ledger = wirestead::diagnostics::SendAccountingLedger;

// A failed admission must leave no trace: later requests are tracked and
// completed normally, and only the requests that were accepted are counted.
TEST(SendAccountingAllocationTest, FailedAdmissionLeavesLedgerUsable) {
  Ledger ledger;
  std::vector<Ledger::Request> accepted{ledger.admit(3)};
  // Node storage is pooled, so keep admitting until an insert must allocate.
  bool threw = false;
  for (int i = 0; i < 100000 && !threw; ++i) {
    t_fail_allocation = true;  // Only around admit(): gtest itself allocates.
    try {
      const auto id = ledger.admit(5);
      t_fail_allocation = false;
      accepted.push_back(id);
    } catch (const std::bad_alloc&) {
      t_fail_allocation = false;
      threw = true;
    }
  }
  ASSERT_TRUE(threw);
  accepted.push_back(ledger.admit(7));
  for (const auto id : accepted) EXPECT_TRUE(ledger.contains(id));
  for (const auto id : accepted) ledger.complete(id, 7);
  const auto s = ledger.snapshot();
  EXPECT_EQ(s.accepted.requests, accepted.size());
  EXPECT_EQ(s.written.requests, accepted.size());
  EXPECT_EQ(s.outstanding.requests, 0u);
}
}  // namespace
