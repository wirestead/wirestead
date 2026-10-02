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
#ifdef _MSC_VER
#include <malloc.h>
#endif
#include <vector>

#include "wirestead/diagnostics/send_accounting.hpp"

// Replaces global operator new for this binary only, so it must stay its own
// executable and is excluded under TSan (see test/unit/CMakeLists.txt).
namespace {
thread_local bool t_fail_allocation = false;

// MSVC has no std::aligned_alloc; its aligned blocks need the matching free.
void* aligned_allocate(std::size_t n, std::size_t align) {
#ifdef _MSC_VER
  return _aligned_malloc(n ? n : 1, align);
#else
  return std::aligned_alloc(align, (n + align - 1) / align * align);
#endif
}
void aligned_release(void* p) noexcept {
#ifdef _MSC_VER
  _aligned_free(p);
#else
  std::free(p);
#endif
}
}  // namespace

void* operator new(std::size_t n) {
  if (t_fail_allocation) throw std::bad_alloc();
  if (void* p = std::malloc(n ? n : 1)) return p;
  throw std::bad_alloc();
}
void* operator new(std::size_t n, std::align_val_t al) {
  if (t_fail_allocation) throw std::bad_alloc();
  const auto align = static_cast<std::size_t>(al);
  if (void* p = aligned_allocate(n, align)) return p;
  throw std::bad_alloc();
}
// Keep every allocation family paired, including gtest's nothrow allocations.
void* operator new[](std::size_t n) { return ::operator new(n); }
void* operator new[](std::size_t n, std::align_val_t al) { return ::operator new(n, al); }
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
  try {
    return ::operator new(n);
  } catch (...) {
    return nullptr;
  }
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
  try {
    return ::operator new[](n);
  } catch (...) {
    return nullptr;
  }
}
void* operator new(std::size_t n, std::align_val_t al, const std::nothrow_t&) noexcept {
  try {
    return ::operator new(n, al);
  } catch (...) {
    return nullptr;
  }
}
void* operator new[](std::size_t n, std::align_val_t al, const std::nothrow_t&) noexcept {
  try {
    return ::operator new[](n, al);
  } catch (...) {
    return nullptr;
  }
}
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { aligned_release(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { aligned_release(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept { aligned_release(p); }
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept { aligned_release(p); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { aligned_release(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { aligned_release(p); }

namespace {
using Ledger = wirestead::diagnostics::SendAccountingLedger;

// A failed admission must leave no trace: later requests are tracked and
// completed normally, and only the requests that were accepted are counted.
TEST(SendAccountingAllocationTest, FailedAdmissionLeavesLedgerUsable) {
  Ledger ledger;
  std::vector<Ledger::Request> accepted{ledger.admit(3)};
  // Keep admitting until the actual ledger storage must allocate.
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
TEST(SendAccountingAllocationTest, RepeatedRollbackDoesNotRetainRetiredIds) {
  Ledger ledger;
  const auto oldest = ledger.admit(1);
  ASSERT_TRUE(ledger.begin(oldest));
  // Warm the small, bounded storage needed for two simultaneous requests.
  for (int i = 0; i < 64; ++i) {
    Ledger::Admission pending(ledger, 1);
  }
  bool threw = false;
  t_fail_allocation = true;
  try {
    for (int i = 0; i < 200000; ++i) {
      Ledger::Admission pending(ledger, 1);
    }
  } catch (const std::bad_alloc&) {
    threw = true;
  }
  t_fail_allocation = false;
  EXPECT_FALSE(threw) << "retired IDs must not force storage growth";
  EXPECT_TRUE(ledger.contains(oldest));
  EXPECT_EQ(ledger.snapshot().accepted.requests, 1u);
  EXPECT_EQ(ledger.snapshot().outstanding.requests, 1u);
  ledger.complete(oldest, 1);
  EXPECT_EQ(ledger.snapshot().written.requests, 1u);
}

TEST(SendAccountingAllocationTest, RepeatedExpiryDoesNotRetainRetiredIds) {
  Ledger ledger;
  const auto oldest = ledger.admit(1);
  ASSERT_TRUE(ledger.begin(oldest));
  const auto group = std::make_shared<Ledger::Group>();
  for (int i = 0; i < 64; ++i) {
    ledger.admit(1, group);
    ledger.discard_waiting(group, Ledger::Cause::SessionExpiry);
  }
  bool threw = false;
  t_fail_allocation = true;
  try {
    for (int i = 0; i < 8192; ++i) {
      ledger.admit(1, group);
      ledger.discard_waiting(group, Ledger::Cause::SessionExpiry);
    }
  } catch (const std::bad_alloc&) {
    threw = true;
  }
  t_fail_allocation = false;
  EXPECT_FALSE(threw) << "expired requests must not force storage growth";
  EXPECT_TRUE(ledger.contains(oldest));
  EXPECT_EQ(ledger.snapshot().outstanding.requests, 1u);
  EXPECT_EQ(ledger.snapshot(group).outstanding.requests, 0u);
  ledger.end(Ledger::Cause::ExplicitStop);
  EXPECT_EQ(ledger.snapshot().explicit_stop.aborted_during_write.requests, 1u);
}

TEST(SendAccountingAllocationTest, SparsePromotionFailurePreservesBothStores) {
  Ledger ledger;
  const auto oldest = ledger.admit(3);
  for (int i = 0; i < 64; ++i) {
    Ledger::Admission pending(ledger, 1);
  }
  const auto next = ledger.admit(5);
  // Fill one ID span with holes while two older requests remain live.
  for (int i = 0; i < 15; ++i) {
    Ledger::Admission pending(ledger, 1);
  }
  bool threw = false;
  t_fail_allocation = true;
  try {
    ledger.admit(7);
  } catch (const std::bad_alloc&) {
    threw = true;
  }
  t_fail_allocation = false;
  EXPECT_TRUE(threw);
  EXPECT_TRUE(ledger.contains(oldest));
  EXPECT_TRUE(ledger.contains(next));
  EXPECT_EQ(ledger.snapshot().accepted.requests, 2u);
  EXPECT_EQ(ledger.snapshot().outstanding.bytes, 8u);
  const auto recovered = ledger.admit(7);
  ledger.complete(oldest, 3);
  ledger.complete(next, 5);
  ledger.complete(recovered, 7);
  EXPECT_EQ(ledger.snapshot().written.requests, 3u);
  EXPECT_EQ(ledger.snapshot().outstanding.requests, 0u);
}
}  // namespace
