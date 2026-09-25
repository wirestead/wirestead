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

#pragma once

#include <cstdint>

namespace wirestead::wrapper {

// Logical requests and their full payload sizes, not I/O batch counts.
struct SendRequestTotals {
  uint64_t requests = 0;
  uint64_t bytes = 0;
};

struct SendLossTotals {
  SendRequestTotals discarded_before_write;
  SendRequestTotals aborted_during_write;
};

struct SendAccounting {
  SendRequestTotals accepted;
  SendRequestTotals written;
  SendRequestTotals outstanding;
  SendLossTotals explicit_stop;
  SendLossTotals connection_loss;
  SendLossTotals queue_pressure;
  // Bytes reported by local completions before a terminal boundary. Neither
  // this nor written proves peer delivery. Late completions cannot revise a
  // request already terminated by stop/loss.
  uint64_t confirmed_written_bytes = 0;
};

}  // namespace wirestead::wrapper
