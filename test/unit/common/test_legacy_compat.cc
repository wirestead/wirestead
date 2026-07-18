#include <gtest/gtest.h>

#include <type_traits>
#include <unilink/diagnostics/exceptions.hpp>
#include <unilink/unilink.hpp>
#include <wirestead/diagnostics/exceptions.hpp>
#include <wirestead/wirestead.hpp>

TEST(LegacyCompat, UnilinkNamespaceAliasesWirestead) {
  static_assert(std::is_same_v<unilink::TcpClient, wirestead::TcpClient>);
  static_assert(std::is_same_v<unilink::builder::TcpClientBuilderDefault, wirestead::builder::TcpClientBuilderDefault>);
}

TEST(LegacyCompat, WiresteadExceptionCatchWorks) {
  try {
    throw wirestead::diagnostics::WiresteadException("canonical");
  } catch (const wirestead::diagnostics::WiresteadException& ex) {
    EXPECT_STREQ(ex.what(), "canonical");
    return;
  }
  FAIL() << "WiresteadException was not caught";
}

TEST(LegacyCompat, UnilinkExceptionCatchWorks) {
  try {
    throw wirestead::diagnostics::WiresteadException("legacy");
  } catch (const unilink::diagnostics::UnilinkException& ex) {
    EXPECT_STREQ(ex.what(), "legacy");
    return;
  }
  FAIL() << "UnilinkException compatibility alias was not caught";
}
