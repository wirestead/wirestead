#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <filesystem>

#include "test_utils.hpp"
#include "wirestead/transport/uds/uds_client.hpp"
#include "wirestead/transport/uds/uds_server.hpp"

using namespace wirestead;
using namespace wirestead::transport;
using namespace wirestead::test;

class UdsErrorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_sock_ = TestUtils::makeUniqueUdsSocketPath("ul_err").string();
    TestUtils::removeFileIfExists(temp_sock_);
  }

  void TearDown() override { TestUtils::removeFileIfExists(temp_sock_); }

  std::string temp_sock_;
};

TEST_F(UdsErrorTest, InvalidSocketPath) {
  config::UdsServerConfig cfg;
  // Extremely long path that exceeds sun_path limit (usually 108 bytes)
  cfg.socket_path = "/tmp/very_long_path_" + std::string(200, 'a') + ".sock";

  auto server = UdsServer::create(cfg);
  ASSERT_NE(server, nullptr);

  // Start should not crash but may set state to Error
  server->start();

  // Give some time for async operation if needed
  TestUtils::waitForCondition([&] { return server->state() == base::LinkState::Error; }, 500);

  // In some implementations, it might stay in Idle if path validation fails immediately
  EXPECT_TRUE(server->state() == base::LinkState::Error || server->state() == base::LinkState::Idle);
  server->stop();
}

TEST_F(UdsErrorTest, PathPermissionDenied) {
#ifdef _WIN32
  GTEST_SKIP() << "UDS file permissions are not consistent on Windows AF_UNIX";
#else
  config::UdsServerConfig cfg;

  // Create a temporary directory and remove all permissions
  std::string restricted_dir = TestUtils::makeUniqueTempFilePath("wirestead_restricted").string();
  std::filesystem::create_directory(restricted_dir);
  std::filesystem::permissions(restricted_dir, std::filesystem::perms::none);

  cfg.socket_path = restricted_dir + "/test.sock";

  auto server = UdsServer::create(cfg);
  ASSERT_NE(server, nullptr);
  server->start();

  // Wait for potential async error transition (longer timeout)
  bool error_state = TestUtils::waitForCondition(
      [&] {
        auto s = server->state();
        return s == base::LinkState::Error || s == base::LinkState::Idle;
      },
      1000);

  EXPECT_TRUE(error_state);
  server->stop();

  // Cleanup: Restore permissions so directory can be deleted
  std::filesystem::permissions(restricted_dir, std::filesystem::perms::owner_all);
  std::filesystem::remove_all(restricted_dir);
#endif
}

TEST_F(UdsErrorTest, ClientConnectWithoutServer) {
  config::UdsClientConfig cfg;
  cfg.socket_path = TestUtils::makeUniqueUdsSocketPath("ul_missing").string();
  cfg.max_retries = 1;
  cfg.retry_interval_ms = 10;

  auto client = UdsClient::create(cfg);
  std::atomic<bool> error_state_seen{false};
  client->on_state([&](base::LinkState state) {
    if (state == base::LinkState::Error) {
      error_state_seen = true;
    }
  });

  client->start();

  // Wait for the async connect attempt to fail before the next UDS test starts.
  EXPECT_TRUE(TestUtils::waitForCondition([&] { return error_state_seen.load(); }, 1000));
  client->stop();
}

TEST_F(UdsErrorTest, ServerStopWithActiveSessions) {
  config::UdsServerConfig scfg;
  scfg.socket_path = temp_sock_;
  auto server = UdsServer::create(scfg);
  server->start();

  // Wait for server to be ready
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return server->state() == base::LinkState::Listening; }, 2000));

  config::UdsClientConfig ccfg;
  ccfg.socket_path = temp_sock_;
  auto client = UdsClient::create(ccfg);
  client->start();

  // Wait until both sides observe the active session before stopping the server.
  ASSERT_TRUE(TestUtils::waitForCondition([&] { return client->is_connected() && server->client_count() == 1; }, 2000));

  // Stop server while client is connected - this must not crash
  server->stop();

  // Just verify server reached Idle or Error state
  EXPECT_TRUE(TestUtils::waitForCondition(
      [&] {
        auto s = server->state();
        return s == base::LinkState::Idle || s == base::LinkState::Error;
      },
      3000));
  EXPECT_EQ(server->client_count(), 0u);

  client->stop();
}
