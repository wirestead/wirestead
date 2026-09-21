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
#include <cstddef>

#include "wirestead/base/constants.hpp"
namespace wirestead::wrapper::detail {
// Invalid payload sizes must reach transport validation immediately rather
// than waiting for queue capacity. The transport still owns rejection,
// error callbacks and failure accounting. A line's size includes its newline.
inline bool payload_needs_capacity(std::size_t size) { return size != 0 && size <= base::constants::MAX_BUFFER_SIZE; }
}  // namespace wirestead::wrapper::detail
