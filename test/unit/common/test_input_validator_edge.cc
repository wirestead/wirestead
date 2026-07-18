#include <gtest/gtest.h>

#include <string>

#include "wirestead/util/input_validator.hpp"

using namespace wirestead::util;

TEST(InputValidatorEdgeTest, IPv6Validation) {
  EXPECT_TRUE(InputValidator::is_valid_ipv6("::1"));
  EXPECT_TRUE(InputValidator::is_valid_ipv6("2001:db8::ff00:42:8329"));
  EXPECT_FALSE(InputValidator::is_valid_ipv6("127.0.0.1"));
  EXPECT_FALSE(InputValidator::is_valid_ipv6("2001:db8::ff00:42:8329:extra"));
  EXPECT_FALSE(InputValidator::is_valid_ipv6(""));
}

TEST(InputValidatorEdgeTest, UdsPathValidation) {
  EXPECT_TRUE(InputValidator::is_valid_uds_path("/tmp/test.sock"));
  EXPECT_FALSE(InputValidator::is_valid_uds_path(""));

  // Test extremely long path
  std::string long_path(200, 'a');
  EXPECT_FALSE(InputValidator::is_valid_uds_path(long_path));
}

TEST(InputValidatorEdgeTest, HostValidation) {
  EXPECT_TRUE(InputValidator::is_valid_host("127.0.0.1"));
  EXPECT_TRUE(InputValidator::is_valid_host("::1"));
  EXPECT_TRUE(InputValidator::is_valid_host("localhost"));
  EXPECT_FALSE(InputValidator::is_valid_host("invalid..host"));
}

TEST(InputValidatorEdgeTest, StringValidationExceptions) {
  EXPECT_THROW(InputValidator::validate_non_empty_string("", "test_field"),
               wirestead::diagnostics::ValidationException);
  EXPECT_NO_THROW(InputValidator::validate_non_empty_string("not empty", "test_field"));

  EXPECT_THROW(InputValidator::validate_string_length("too long", 5, "test_field"),
               wirestead::diagnostics::ValidationException);
  EXPECT_NO_THROW(InputValidator::validate_string_length("short", 10, "test_field"));
}

TEST(InputValidatorEdgeTest, NumericValidationExceptions) {
  EXPECT_THROW(InputValidator::validate_positive_number(0, "test_field"), wirestead::diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_positive_number(-1, "test_field"), wirestead::diagnostics::ValidationException);
  EXPECT_NO_THROW(InputValidator::validate_positive_number(1, "test_field"));

  EXPECT_THROW(InputValidator::validate_range(static_cast<int64_t>(5), static_cast<int64_t>(10),
                                              static_cast<int64_t>(20), "test_field"),
               wirestead::diagnostics::ValidationException);
  EXPECT_THROW(InputValidator::validate_range(static_cast<int64_t>(25), static_cast<int64_t>(10),
                                              static_cast<int64_t>(20), "test_field"),
               wirestead::diagnostics::ValidationException);
  EXPECT_NO_THROW(InputValidator::validate_range(static_cast<int64_t>(15), static_cast<int64_t>(10),
                                                 static_cast<int64_t>(20), "test_field"));
}
TEST(InputValidatorEdgeTest, RetryValidation) {
  EXPECT_NO_THROW(InputValidator::validate_retry_count(-1));  // infinite
  EXPECT_NO_THROW(InputValidator::validate_retry_count(0));
  EXPECT_NO_THROW(InputValidator::validate_retry_count(100));
  EXPECT_THROW(InputValidator::validate_retry_count(-2), wirestead::diagnostics::ValidationException);
}
