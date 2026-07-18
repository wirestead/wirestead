#!/usr/bin/env bash
set -euo pipefail

OUTPUT_JSON="benchmark-result.json"
OUTPUT_SUMMARY="benchmark-summary.md"
CMAKE_PREFIX_PATH_VALUE="${CMAKE_PREFIX_PATH:-}"
CMAKE_GENERATOR="${CMAKE_GENERATOR:-Ninja}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"

usage() {
  cat <<'EOF'
Usage: scripts/run_benchmarks.sh [options]

Options:
  --output-json PATH       JSON output path
  --output-summary PATH    Markdown summary output path
  --cmake-prefix PATH      Installed wirestead CMake package prefix
  --help                   Show this help message
EOF
}

require_arg() {
  local option="$1"
  local value="${2:-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    echo "error: $option requires a value" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-json)
      require_arg "$1" "${2:-}"
      OUTPUT_JSON="$2"
      shift 2
      ;;
    --output-summary)
      require_arg "$1" "${2:-}"
      OUTPUT_SUMMARY="$2"
      shift 2
      ;;
    --cmake-prefix)
      require_arg "$1" "${2:-}"
      CMAKE_PREFIX_PATH_VALUE="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$CMAKE_PREFIX_PATH_VALUE" ]]; then
  echo "error: --cmake-prefix or CMAKE_PREFIX_PATH is required" >&2
  exit 1
fi

OUTPUT_JSON="$(realpath -m "$OUTPUT_JSON")"
OUTPUT_SUMMARY="$(realpath -m "$OUTPUT_SUMMARY")"
WORK_DIR="$(dirname "$OUTPUT_JSON")/benchmark-work"
SRC_DIR="$WORK_DIR/src"
BUILD_DIR="$WORK_DIR/build"

rm -rf "$WORK_DIR"
mkdir -p "$SRC_DIR" "$BUILD_DIR" "$(dirname "$OUTPUT_SUMMARY")"

cat > "$SRC_DIR/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.12)
project(wirestead_benchmark_smoke LANGUAGES CXX)

find_package(wirestead CONFIG REQUIRED)

add_executable(wirestead_benchmark_smoke main.cpp)
target_link_libraries(wirestead_benchmark_smoke PRIVATE wirestead::wirestead)
target_compile_features(wirestead_benchmark_smoke PRIVATE cxx_std_20)
EOF

cat > "$SRC_DIR/main.cpp" <<'EOF'
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <wirestead/wirestead.hpp>

using Clock = std::chrono::steady_clock;

uint16_t reserve_tcp_port() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return 0;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return 0;
  }
  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
    ::close(fd);
    return 0;
  }
  auto port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

template <typename Predicate>
bool wait_until(Predicate&& predicate, std::chrono::milliseconds timeout) {
  const auto deadline = Clock::now() + timeout;
  while (Clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  return false;
}

double percentile_us(std::vector<double> values, double percentile) {
  if (values.empty()) return 0.0;
  std::sort(values.begin(), values.end());
  const auto index = static_cast<size_t>((percentile / 100.0) * static_cast<double>(values.size() - 1));
  return values[index];
}

struct ThroughputMetric {
  size_t payload_size;
  int iterations;
  double mbps;
};

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: wirestead_benchmark_smoke result.json summary.md\n";
    return 2;
  }

  const auto port = reserve_tcp_port();
  if (port == 0) {
    std::cerr << "failed to reserve TCP port\n";
    return 3;
  }

  std::atomic<uint64_t> server_received{0};
  std::atomic<uint64_t> server_received_bytes{0};
  std::atomic<uint64_t> client_received{0};
  std::shared_ptr<wirestead::TcpServer> server;

  server = wirestead::tcp_server(port)
               .auto_start(false)
               .on_data([&](const wirestead::MessageContext& ctx) {
                 server_received.fetch_add(1, std::memory_order_relaxed);
                 server_received_bytes.fetch_add(ctx.data().size(), std::memory_order_relaxed);
                 if (server) server->send_to(ctx.client_id(), "ack");
               })
               .on_error([](const wirestead::ErrorContext&) {})
               .build();
  auto client = wirestead::tcp_client("127.0.0.1", port)
                    .auto_start(false)
                    .on_data([&](const wirestead::MessageContext&) {
                      client_received.fetch_add(1, std::memory_order_relaxed);
                    })
                    .on_error([](const wirestead::ErrorContext&) {})
                    .build();

  if (!server || !server->start_sync() || !client || !client->start_sync()) {
    std::cerr << "failed to start benchmark loopback\n";
    return 4;
  }
  if (!wait_until([&]() { return client->connected() && server->client_count() >= 1; }, std::chrono::seconds(5))) {
    std::cerr << "benchmark loopback did not connect\n";
    return 5;
  }

  std::vector<double> latency_us;
  latency_us.reserve(200);
  for (int i = 0; i < 200; ++i) {
    const auto target = client_received.load(std::memory_order_relaxed) + 1;
    const auto start = Clock::now();
    if (!client->send("latency")) return 6;
    if (!wait_until([&]() { return client_received.load(std::memory_order_relaxed) >= target; },
                    std::chrono::seconds(2))) {
      return 7;
    }
    const auto elapsed = std::chrono::duration<double, std::micro>(Clock::now() - start).count();
    latency_us.push_back(elapsed);
  }

  std::vector<ThroughputMetric> throughput;
  for (size_t payload_size : {size_t{64}, size_t{1024}, size_t{16 * 1024}, size_t{64 * 1024}, size_t{1024 * 1024}}) {
    const int iterations = payload_size >= 1024 * 1024 ? 4 : (payload_size >= 64 * 1024 ? 16 : 100);
    const std::string payload(payload_size, 'x');
    const auto target = server_received_bytes.load(std::memory_order_relaxed) +
                        static_cast<uint64_t>(payload_size * iterations);
    const auto start = Clock::now();
    for (int i = 0; i < iterations; ++i) {
      if (!client->send(payload)) return 8;
    }
    if (!wait_until([&]() { return server_received_bytes.load(std::memory_order_relaxed) >= target; },
                    std::chrono::seconds(10))) {
      return 9;
    }
    const auto seconds = std::chrono::duration<double>(Clock::now() - start).count();
    const auto mbps = (static_cast<double>(payload_size) * iterations) / seconds / (1024.0 * 1024.0);
    throughput.push_back({payload_size, iterations, mbps});
  }

  const int variant_iterations = 100;
  const std::string payload = "variant";
  int send_ok = 0;
  int try_send_ok = 0;
  int send_move_ok = 0;
  int send_shared_ok = 0;
  for (int i = 0; i < variant_iterations; ++i) {
    if (client->send(payload)) ++send_ok;
    if (client->try_send(payload)) ++try_send_ok;
    if (client->send_move(std::vector<uint8_t>(payload.begin(), payload.end()))) ++send_move_ok;
    if (client->send_shared(std::make_shared<const std::vector<uint8_t>>(
            std::vector<uint8_t>(payload.begin(), payload.end())))) {
      ++send_shared_ok;
    }
  }

  const auto p50 = percentile_us(latency_us, 50.0);
  const auto p95 = percentile_us(latency_us, 95.0);
  const auto p99 = percentile_us(latency_us, 99.0);
  const auto stats = client->stats();

  server->stop();
  client->stop();

  std::ofstream json(argv[1]);
  json << std::fixed << std::setprecision(3);
  json << "{\n";
  json << "  \"status\": \"ok\",\n";
  json << "  \"soft_gate\": true,\n";
  json << "  \"tcp_loopback_latency_us\": {\"p50\": " << p50 << ", \"p95\": " << p95 << ", \"p99\": " << p99
       << "},\n";
  json << "  \"throughput_mib_per_sec\": [\n";
  for (size_t i = 0; i < throughput.size(); ++i) {
    const auto& row = throughput[i];
    json << "    {\"payload_size\": " << row.payload_size << ", \"iterations\": " << row.iterations
         << ", \"value\": " << row.mbps << "}";
    json << (i + 1 == throughput.size() ? "\n" : ",\n");
  }
  json << "  ],\n";
  json << "  \"send_variants\": {\"iterations\": " << variant_iterations << ", \"send\": " << send_ok
       << ", \"try_send\": " << try_send_ok << ", \"send_move\": " << send_move_ok
       << ", \"send_shared\": " << send_shared_ok << "},\n";
  json << "  \"queue_snapshot\": {\"queued_bytes\": " << stats.queued_bytes
       << ", \"pending_bytes\": " << stats.pending_bytes << ", \"dropped_bytes\": " << stats.dropped_bytes
       << "},\n";
  json << "  \"notes\": [\"TCP loopback smoke benchmark; compare against release baseline before gating.\"]\n";
  json << "}\n";

  std::ofstream summary(argv[2]);
  summary << "# Benchmark Summary\n\n";
  summary << "| Metric | Value |\n";
  summary << "| --- | ---: |\n";
  summary << "| TCP latency p50 | " << p50 << " us |\n";
  summary << "| TCP latency p95 | " << p95 << " us |\n";
  summary << "| TCP latency p99 | " << p99 << " us |\n";
  for (const auto& row : throughput) {
    summary << "| Throughput " << row.payload_size << " B | " << row.mbps << " MiB/s |\n";
  }
  summary << "| send accepted | " << send_ok << "/" << variant_iterations << " |\n";
  summary << "| try_send accepted | " << try_send_ok << "/" << variant_iterations << " |\n";
  summary << "| send_move accepted | " << send_move_ok << "/" << variant_iterations << " |\n";
  summary << "| send_shared accepted | " << send_shared_ok << "/" << variant_iterations << " |\n";
  summary << "\nSoft gate only: compare p99 latency, throughput, and queue growth with the current release baseline.\n";

  return 0;
}
EOF

cmake -S "$SRC_DIR" -B "$BUILD_DIR" -G "$CMAKE_GENERATOR" \
  -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE" \
  -DCMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH_VALUE"
cmake --build "$BUILD_DIR" --parallel
"$BUILD_DIR/wirestead_benchmark_smoke" "$OUTPUT_JSON" "$OUTPUT_SUMMARY"
