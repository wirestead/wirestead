#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <chrono>
#include <memory>
#include <thread>

#include "wirestead/base/common.hpp"
#include "wirestead/config/udp_config.hpp"
#include "wirestead/memory/safe_span.hpp"
#include "wirestead/wrapper/udp/udp.hpp"

using namespace wirestead;
using namespace std::chrono_literals;

TEST(UdpWrapperTest, StopCompletesAfterStart) {
  config::UdpConfig cfg;
  cfg.local_port = 0;
  cfg.remote_address = "127.0.0.1";
  cfg.remote_port = 19001;

  wrapper::UdpClient udp(cfg);
  auto start_result = udp.start();
  ASSERT_TRUE(start_result.get());

  EXPECT_NO_THROW(udp.stop());
}

TEST(UdpWrapperTest, StopSafetyWithExternalIOC) {
  auto ioc = std::make_shared<boost::asio::io_context>();
  auto work_guard = boost::asio::make_work_guard(*ioc);
  std::thread io_thread([ioc] { ioc->run(); });

  config::UdpConfig cfg;
  cfg.local_port = 0;

  {
    wrapper::UdpClient udp(cfg, ioc);
    std::atomic<int> callbacks{0};
    udp.on_data([&](const wrapper::MessageContext&) { callbacks++; });
    auto f = udp.start();

    // Stop and destroy immediately
    udp.stop();
  }

  // Wait a bit to ensure no late callbacks cause crashes
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  work_guard.reset();
  ioc->stop();
  if (io_thread.joinable()) io_thread.join();
}
