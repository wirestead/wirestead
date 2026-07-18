#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <iostream>
#include <vector>

#include "wirestead/diagnostics/error_types.hpp"

using namespace wirestead::diagnostics;

TEST(ErrorMappingExhaustiveTest, ErrorInfoFormatting) {
  // Test basic construction and string conversion
  ErrorInfo info(ErrorLevel::ERROR, ErrorCategory::COMMUNICATION, "test_comp", "read", "test message");

  EXPECT_EQ(info.level_string(), "ERROR");
  EXPECT_EQ(info.category_string(), "COMMUNICATION");
  EXPECT_NE(info.summary(), "");
  EXPECT_NE(info.timestamp_string(), "");

  // Test with boost error
  boost::system::error_code ec = boost::asio::error::connection_reset;
  ErrorInfo info2(ErrorLevel::CRITICAL, ErrorCategory::CONNECTION, "tcp", "connect", "reset by peer", ec, true);

  EXPECT_EQ(info2.level_string(), "CRITICAL");
  EXPECT_TRUE(info2.retryable);
  EXPECT_EQ(info2.boost_error, ec);
  EXPECT_TRUE(info2.summary().find("connection_reset") != std::string::npos ||
              info2.summary().find("reset") != std::string::npos);
}

TEST(ErrorMappingExhaustiveTest, EnumCoverage) {
  // Directly exercise all enum cases to ensure 100% branch coverage in switch statements
  std::vector<ErrorLevel> levels = {ErrorLevel::INFO, ErrorLevel::WARNING, ErrorLevel::ERROR, ErrorLevel::CRITICAL};
  for (auto l : levels) {
    ErrorInfo info(l, ErrorCategory::UNKNOWN, "c", "o", "m");
    EXPECT_FALSE(info.level_string().empty());
  }

  std::vector<ErrorCategory> cats = {ErrorCategory::CONNECTION,    ErrorCategory::COMMUNICATION,
                                     ErrorCategory::CONFIGURATION, ErrorCategory::MEMORY,
                                     ErrorCategory::SYSTEM,        ErrorCategory::UNKNOWN};
  for (auto c : cats) {
    ErrorInfo info(ErrorLevel::INFO, c, "c", "o", "m");
    EXPECT_FALSE(info.category_string().empty());
  }
}

TEST(ErrorMappingExhaustiveTest, ErrorStatsTest) {
  ErrorStats stats;
  stats.reset();
  EXPECT_EQ(stats.total_errors, 0);
  EXPECT_EQ(stats.error_rate(), 0.0);

  stats.total_errors = 10;
  stats.first_error = std::chrono::system_clock::now() - std::chrono::minutes(5);
  stats.last_error = std::chrono::system_clock::now();
  EXPECT_GT(stats.error_rate(), 0.0);
}
