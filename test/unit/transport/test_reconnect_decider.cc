#include <gtest/gtest.h>

#include <chrono>
#include <deque>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#include "wirestead/config/tcp_client_config.hpp"
#include "wirestead/diagnostics/error_types.hpp"
#include "wirestead/transport/base/bp_utils.hpp"
#include "wirestead/transport/base/reconnect_policy.hpp"
#include "wirestead/transport/tcp_client/detail/reconnect_decider.hpp"

using namespace wirestead;
using namespace wirestead::transport::detail;
using namespace std::chrono_literals;

namespace {

diagnostics::ErrorInfo MakeErrorInfo(bool retryable) {
  return diagnostics::ErrorInfo(diagnostics::ErrorLevel::ERROR, diagnostics::ErrorCategory::CONNECTION, "test",
                                "connect", "simulated error", boost::system::error_code{}, retryable);
}

config::TcpClientConfig MakeBaseConfig() {
  config::TcpClientConfig cfg;
  cfg.max_retries = -1;
  cfg.retry_interval_ms = 3000;
  return cfg;
}

}  // namespace

TEST(ReconnectDeciderTest, MaxRetriesZeroAlwaysStopsEvenIfPolicyWouldRetry) {
  auto cfg = MakeBaseConfig();
  cfg.max_retries = 0;
  auto info = MakeErrorInfo(true);

  bool policy_called = false;
  ReconnectPolicy policy = [&policy_called](const diagnostics::ErrorInfo&, uint32_t) -> ReconnectDecision {
    policy_called = true;
    return {true, 1ms};
  };

  auto decision = decide_reconnect(cfg, info, 0, policy);
  EXPECT_FALSE(decision.should_retry);
  EXPECT_FALSE(policy_called);
}

TEST(ReconnectDeciderTest, MaxRetriesBoundaryStopsAtAttemptCountEqualToLimit) {
  auto cfg = MakeBaseConfig();
  cfg.max_retries = 3;
  auto info = MakeErrorInfo(true);

  auto decision = decide_reconnect(cfg, info, 3, std::nullopt);
  EXPECT_FALSE(decision.should_retry);
}

TEST(ReconnectDeciderTest, MaxRetriesAllowsAttemptsBelowLimit) {
  auto cfg = MakeBaseConfig();
  cfg.max_retries = 3;
  auto info = MakeErrorInfo(true);

  EXPECT_TRUE(decide_reconnect(cfg, info, 0, std::nullopt).should_retry);
  EXPECT_TRUE(decide_reconnect(cfg, info, 1, std::nullopt).should_retry);
  EXPECT_TRUE(decide_reconnect(cfg, info, 2, std::nullopt).should_retry);
}

TEST(ReconnectDeciderTest, NonRetryableErrorStopsEvenIfPolicyWouldRetry) {
  auto cfg = MakeBaseConfig();
  auto info = MakeErrorInfo(false);

  bool policy_called = false;
  ReconnectPolicy policy = [&policy_called](const diagnostics::ErrorInfo&, uint32_t) -> ReconnectDecision {
    policy_called = true;
    return {true, 1ms};
  };

  auto decision = decide_reconnect(cfg, info, 0, policy);
  EXPECT_FALSE(decision.should_retry);
  EXPECT_FALSE(policy_called);
}

TEST(ReconnectDeciderTest, PolicyRetryDelayIsUsed) {
  auto cfg = MakeBaseConfig();
  auto info = MakeErrorInfo(true);
  ReconnectPolicy policy = [](const diagnostics::ErrorInfo&, uint32_t) -> ReconnectDecision { return {true, 100ms}; };

  auto decision = decide_reconnect(cfg, info, 0, policy);
  ASSERT_TRUE(decision.should_retry);
  ASSERT_TRUE(decision.delay.has_value());
  EXPECT_EQ(*decision.delay, 100ms);
}

TEST(ReconnectDeciderTest, PolicyNegativeDelayClampsToZero) {
  auto cfg = MakeBaseConfig();
  auto info = MakeErrorInfo(true);
  ReconnectPolicy policy = [](const diagnostics::ErrorInfo&, uint32_t) -> ReconnectDecision { return {true, -10ms}; };

  auto decision = decide_reconnect(cfg, info, 0, policy);
  ASSERT_TRUE(decision.should_retry);
  ASSERT_TRUE(decision.delay.has_value());
  EXPECT_EQ(*decision.delay, 0ms);
}

TEST(ReconnectDeciderTest, PolicyHugeDelayClampsToConfiguredCap) {
  auto cfg = MakeBaseConfig();
  auto info = MakeErrorInfo(true);
  ReconnectPolicy policy = [](const diagnostics::ErrorInfo&, uint32_t) -> ReconnectDecision {
    return {true, 999999ms};
  };

  auto decision = decide_reconnect(cfg, info, 0, policy);
  ASSERT_TRUE(decision.should_retry);
  ASSERT_TRUE(decision.delay.has_value());
  EXPECT_LE(*decision.delay, 60s);
  EXPECT_EQ(*decision.delay, MAX_RECONNECT_DELAY);
}

TEST(ReconnectDeciderTest, NoPolicyFallsBackToRetryInterval) {
  auto cfg = MakeBaseConfig();
  cfg.retry_interval_ms = 1500;
  auto info = MakeErrorInfo(true);

  auto decision = decide_reconnect(cfg, info, 0, std::nullopt);
  ASSERT_TRUE(decision.should_retry);
  ASSERT_TRUE(decision.delay.has_value());
  EXPECT_EQ(*decision.delay, 1500ms);
}

TEST(ReconnectDeciderTest, PolicyCanStopReconnect) {
  auto cfg = MakeBaseConfig();
  auto info = MakeErrorInfo(true);
  ReconnectPolicy policy = [](const diagnostics::ErrorInfo&, uint32_t) -> ReconnectDecision { return {false, 1ms}; };

  auto decision = decide_reconnect(cfg, info, 0, policy);
  EXPECT_FALSE(decision.should_retry);
  EXPECT_FALSE(decision.delay.has_value());
}

TEST(ReconnectPolicyTest, FixedIntervalRespectsRetryableFlag) {
  auto retryable = MakeErrorInfo(true);
  auto non_retryable = MakeErrorInfo(false);
  auto policy = FixedInterval(42ms);

  auto retry = policy(retryable, 3);
  EXPECT_TRUE(retry.retry);
  EXPECT_EQ(retry.delay, 42ms);

  auto stop = policy(non_retryable, 3);
  EXPECT_FALSE(stop.retry);
  EXPECT_EQ(stop.delay, 0ms);
}

TEST(ReconnectPolicyTest, ExponentialBackoffWithoutJitterCapsDelay) {
  auto retryable = MakeErrorInfo(true);
  auto non_retryable = MakeErrorInfo(false);
  auto policy = ExponentialBackoff(10ms, 100ms, 3.0, false);

  auto first = policy(retryable, 0);
  EXPECT_TRUE(first.retry);
  EXPECT_EQ(first.delay, 10ms);

  auto third = policy(retryable, 2);
  EXPECT_TRUE(third.retry);
  EXPECT_EQ(third.delay, 90ms);

  auto capped = policy(retryable, 3);
  EXPECT_TRUE(capped.retry);
  EXPECT_EQ(capped.delay, 100ms);

  auto stop = policy(non_retryable, 3);
  EXPECT_FALSE(stop.retry);
  EXPECT_EQ(stop.delay, 0ms);
}

TEST(ReconnectPolicyTest, ExponentialBackoffWithJitterStaysWithinCurrentCap) {
  auto retryable = MakeErrorInfo(true);
  auto policy = ExponentialBackoff(20ms, 100ms, 2.0, true);

  auto decision = policy(retryable, 2);
  EXPECT_TRUE(decision.retry);
  EXPECT_GE(decision.delay, 0ms);
  EXPECT_LE(decision.delay, 80ms);
}

TEST(BackpressureQueueUtilTest, VariantBufferSizeHandlesVectorAndSharedPointer) {
  using wirestead::transport::queue_util::variant_buffer_size;

  std::vector<uint8_t> vector_buffer{1, 2, 3};
  auto shared_buffer = std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{1, 2, 3, 4});
  std::shared_ptr<const std::vector<uint8_t>> null_buffer;

  EXPECT_EQ(variant_buffer_size(vector_buffer), 3);
  EXPECT_EQ(variant_buffer_size(shared_buffer), 4);
  EXPECT_EQ(variant_buffer_size(null_buffer), 0);
}

TEST(BackpressureQueueUtilTest, ReliableStrategyDoesNotTrimQueue) {
  using Buffer = std::variant<std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;
  using wirestead::transport::queue_util::maybe_flush_for_keep_latest;

  std::deque<Buffer> tx;
  tx.emplace_back(std::vector<uint8_t>{1, 2, 3});
  tx.emplace_back(std::vector<uint8_t>{4, 5, 6, 7});
  std::atomic<size_t> queue_bytes{7};
  std::atomic<bool> backpressure_active{true};

  const auto dropped = maybe_flush_for_keep_latest(base::constants::BackpressureStrategy::Reliable, 10, 5, tx,
                                                   queue_bytes, backpressure_active);

  EXPECT_EQ(tx.size(), 2);
  EXPECT_EQ(queue_bytes.load(), 7);
  EXPECT_EQ(dropped.messages, 0u);
  EXPECT_EQ(dropped.bytes, 0u);
}

TEST(BackpressureQueueUtilTest, BestEffortDoesNotDropBelowHighWatermark) {
  using Buffer = std::variant<std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;
  using wirestead::transport::queue_util::maybe_flush_for_keep_latest;

  std::deque<Buffer> tx;
  tx.emplace_back(std::vector<uint8_t>{1, 2, 3});
  tx.emplace_back(std::vector<uint8_t>{4, 5});
  std::atomic<size_t> queue_bytes{5};
  std::atomic<bool> backpressure_active{false};

  const auto dropped = maybe_flush_for_keep_latest(base::constants::BackpressureStrategy::BestEffort, 4, 10, tx,
                                                   queue_bytes, backpressure_active);

  EXPECT_EQ(tx.size(), 2);
  EXPECT_EQ(queue_bytes.load(), 5);
  EXPECT_EQ(dropped.messages, 0u);
  EXPECT_EQ(dropped.bytes, 0u);
}

TEST(BackpressureQueueUtilTest, BestEffortDropsQueueWhenAddedBufferExceedsHighWatermark) {
  using Buffer = std::variant<std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;
  using wirestead::transport::queue_util::maybe_flush_for_keep_latest;

  std::deque<Buffer> tx;
  tx.emplace_back(std::vector<uint8_t>{1, 2, 3, 4});
  tx.emplace_back(std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{5, 6, 7, 8, 9}));
  std::atomic<size_t> queue_bytes{9};
  std::atomic<bool> backpressure_active{false};

  const auto dropped = maybe_flush_for_keep_latest(base::constants::BackpressureStrategy::BestEffort, 10, 10, tx,
                                                   queue_bytes, backpressure_active);

  EXPECT_TRUE(tx.empty());
  EXPECT_EQ(queue_bytes.load(), 0);
  EXPECT_EQ(dropped.messages, 2u);
  EXPECT_EQ(dropped.bytes, 9u);
}

TEST(BackpressureQueueUtilTest, BestEffortTrimsOldestBuffersUntilAddedBufferFits) {
  using Buffer = std::variant<std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;
  using wirestead::transport::queue_util::maybe_flush_for_keep_latest;

  std::deque<Buffer> tx;
  tx.emplace_back(std::vector<uint8_t>{1, 2, 3, 4});
  tx.emplace_back(std::vector<uint8_t>{5, 6, 7, 8});
  tx.emplace_back(std::vector<uint8_t>{9, 10, 11, 12});
  std::atomic<size_t> queue_bytes{12};
  std::atomic<bool> backpressure_active{false};

  const auto dropped = maybe_flush_for_keep_latest(base::constants::BackpressureStrategy::BestEffort, 3, 10, tx,
                                                   queue_bytes, backpressure_active);

  ASSERT_EQ(tx.size(), 1);
  EXPECT_EQ(queue_bytes.load(), 4);
  EXPECT_EQ(dropped.messages, 2u);
  EXPECT_EQ(dropped.bytes, 8u);
}

TEST(BackpressureQueueUtilTest, BestEffortOnlyCountsBuffersStillQueued) {
  using Buffer = std::variant<std::vector<uint8_t>, std::shared_ptr<const std::vector<uint8_t>>>;
  using wirestead::transport::queue_util::maybe_flush_for_keep_latest;

  const size_t in_flight_bytes = 6;
  std::deque<Buffer> tx;
  tx.emplace_back(std::vector<uint8_t>{1, 2, 3, 4});
  tx.emplace_back(std::vector<uint8_t>{5, 6});
  std::atomic<size_t> queue_bytes{in_flight_bytes + 6};
  std::atomic<bool> backpressure_active{false};

  const auto dropped = maybe_flush_for_keep_latest(base::constants::BackpressureStrategy::BestEffort, 5, 10, tx,
                                                   queue_bytes, backpressure_active);

  EXPECT_TRUE(tx.empty());
  EXPECT_EQ(queue_bytes.load(), in_flight_bytes);
  EXPECT_EQ(dropped.messages, 2u);
  EXPECT_EQ(dropped.bytes, 6u);
}
