#include <gtest/gtest.h>

#include <sstream>
#include <thread>
#include <vector>

#include "wirestead/diagnostics/logger.hpp"
#include "wirestead/memory/memory_tracker.hpp"
#include "wirestead/memory/safe_data_buffer.hpp"

using namespace wirestead::memory;

class MemoryTrackerAndBufferTest : public ::testing::Test {
 protected:
  void SetUp() override {
    MemoryTracker::instance().clear_tracking_data();
    MemoryTracker::instance().enable_tracking(true);
  }

  void TearDown() override { MemoryTracker::instance().clear_tracking_data(); }
};

TEST_F(MemoryTrackerAndBufferTest, MemoryTrackerDetailedStats) {
  auto& tracker = MemoryTracker::instance();
  void* ptr1 = (void*)0x1234;
  void* ptr2 = (void*)0x5678;

  tracker.track_allocation(ptr1, 100, __FILE__, __LINE__, __FUNCTION__);
  tracker.track_allocation(ptr2, 200, __FILE__, __LINE__, __FUNCTION__);

  auto stats = tracker.stats();
  EXPECT_EQ(stats.total_allocations, 2);
  EXPECT_EQ(stats.current_bytes_allocated, 300);
  EXPECT_EQ(stats.peak_bytes_allocated, 300);

  tracker.track_deallocation(ptr1);
  stats = tracker.stats();
  EXPECT_EQ(stats.total_deallocations, 1);
  EXPECT_EQ(stats.current_bytes_allocated, 200);
}

TEST_F(MemoryTrackerAndBufferTest, MemoryTrackerControl) {
  auto& tracker = MemoryTracker::instance();
  tracker.disable_tracking();
  EXPECT_FALSE(tracker.tracking_enabled());

  void* ptr = (void*)0x9999;
  tracker.track_allocation(ptr, 500, __FILE__, __LINE__, __FUNCTION__);
  EXPECT_EQ(tracker.stats().total_allocations, 0);

  tracker.enable_tracking();
  tracker.track_allocation(ptr, 500, __FILE__, __LINE__, __FUNCTION__);
  EXPECT_EQ(tracker.stats().total_allocations, 1);
}

TEST_F(MemoryTrackerAndBufferTest, ScopedTracker) {
  auto& tracker = MemoryTracker::instance();
  void* ptr = (void*)0xAAAA;

  {
    ScopedMemoryTracker scoped(__FILE__, __LINE__, __FUNCTION__);
    scoped.track_allocation(ptr, 150);
    EXPECT_EQ(tracker.stats().current_bytes_allocated, 150);
    scoped.track_deallocation(ptr);
    EXPECT_EQ(tracker.stats().current_bytes_allocated, 0);
  }
}

TEST_F(MemoryTrackerAndBufferTest, ReportingMethods) {
  auto& tracker = MemoryTracker::instance();
  tracker.track_allocation((void*)0x1, 10, nullptr, 0, nullptr);

  // These print to stdout or log. Just ensuring they don't crash.
  EXPECT_NO_THROW(tracker.print_memory_report());
  EXPECT_NO_THROW(tracker.print_leak_report());
  EXPECT_NO_THROW(tracker.log_memory_report());
  EXPECT_NO_THROW(tracker.log_leak_report());
}

TEST_F(MemoryTrackerAndBufferTest, SafeDataBufferComprehensive) {
  std::string original = "Wirestead Safe Buffer Test";
  SafeDataBuffer buffer(original);

  EXPECT_FALSE(buffer.empty());
  EXPECT_EQ(buffer.size(), original.size());
  EXPECT_EQ(buffer.as_string(), original);

  // Access
  EXPECT_EQ(buffer[0], 'W');
  EXPECT_EQ(buffer.at(original.size() - 1), 't');
  EXPECT_THROW(buffer.at(original.size()), std::out_of_range);
  EXPECT_THROW(buffer[original.size()], std::out_of_range);

  // Comparison
  SafeDataBuffer buffer2(original);
  EXPECT_EQ(buffer, buffer2);

  SafeDataBuffer buffer3(std::string("Different"));
  EXPECT_NE(buffer, buffer3);

  // Lifecycle
  buffer.clear();
  EXPECT_TRUE(buffer.empty());
  EXPECT_EQ(buffer.size(), 0);

  buffer.reserve(100);
  buffer.resize(10);
  EXPECT_EQ(buffer.size(), 10);
}

TEST_F(MemoryTrackerAndBufferTest, SafeDataBufferErrorCases) {
  // Null pointer with size > 0
  EXPECT_THROW(SafeDataBuffer(static_cast<const uint8_t*>(nullptr), 10), std::invalid_argument);
  EXPECT_THROW(SafeDataBuffer(static_cast<const char*>(nullptr), 10), std::invalid_argument);

  // Size limit (100MB)
  size_t too_large = 1024 * 1024 * 101;
  // Note: we don't actually allocate the memory, just pass the size to trigger validation
  EXPECT_THROW(SafeDataBuffer(reinterpret_cast<const uint8_t*>(0x1), too_large), std::invalid_argument);

  // Validate method
  SafeDataBuffer buffer(std::vector<uint8_t>(10, 0));
  EXPECT_NO_THROW(buffer.validate());
  EXPECT_TRUE(buffer.valid());
}

TEST_F(MemoryTrackerAndBufferTest, SafeDataBufferFactory) {
  using namespace safe_buffer_factory;

  EXPECT_EQ(from_string("test").as_string(), "test");
  EXPECT_EQ(from_c_string("test").as_string(), "test");
  EXPECT_TRUE(from_c_string(nullptr).empty());

  std::vector<uint8_t> vec = {1, 2, 3};
  EXPECT_EQ(from_vector(vec).size(), 3);
  EXPECT_EQ(from_raw_data(vec.data(), 3).size(), 3);
  EXPECT_EQ(from_span(ConstByteSpan(vec.data(), 3)).size(), 3);
}

TEST_F(MemoryTrackerAndBufferTest, MemoryTrackerLeakDetection) {
  auto& tracker = MemoryTracker::instance();
  tracker.track_allocation((void*)0xBBBB, 50, __FILE__, __LINE__, __FUNCTION__);

  auto leaks = tracker.leaked_allocations();
  ASSERT_EQ(leaks.size(), 1);
  EXPECT_EQ(leaks[0].size, 50);
  EXPECT_EQ(leaks[0].ptr, (void*)0xBBBB);
}

// ============================================================================
// BASE UTILITY TESTS (For extra coverage)
// ============================================================================

#include "wirestead/base/common.hpp"

TEST(BaseUtilityTest, SafeMemcpy) {
  using namespace wirestead::base::safe_memory;
  uint8_t src[] = {1, 2, 3, 4, 5};
  uint8_t dest[5];

  EXPECT_NO_THROW(safe_memcpy(dest, src, 5));
  EXPECT_EQ(dest[0], 1);

  EXPECT_THROW(safe_memcpy(nullptr, src, 5), std::invalid_argument);
  EXPECT_THROW(safe_memcpy(dest, nullptr, 5), std::invalid_argument);
  EXPECT_THROW(safe_memcpy(dest, src, wirestead::base::constants::MAX_BUFFER_SIZE + 1), std::invalid_argument);
  EXPECT_NO_THROW(safe_memcpy(dest, src, 0));
}

TEST(BaseUtilityTest, SafeConvert) {
  using namespace wirestead::base::safe_convert;
  std::string s = "test";

  auto vec = string_to_uint8(s);
  EXPECT_EQ(vec.size(), 4);
  EXPECT_EQ(vec[0], 't');

  EXPECT_EQ(uint8_to_string(vec.data(), vec.size()), s);
  EXPECT_EQ(uint8_to_string(nullptr, 0), "");

  auto bytes = string_to_bytes(s);
  EXPECT_EQ(bytes.second, 4);
  EXPECT_EQ(bytes.first[0], 't');
}
