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

#if defined(_MSC_VER)
#define WIRESTEAD_DEPRECATED_MSG(msg) __declspec(deprecated(msg))
#elif defined(__GNUC__) || defined(__clang__)
#define WIRESTEAD_DEPRECATED_MSG(msg) __attribute__((deprecated(msg)))
#else
#define WIRESTEAD_DEPRECATED_MSG(msg)
#endif

// Fallback or alias if desired, but prioritize safety
#ifndef WIRESTEAD_DEPRECATED
#define WIRESTEAD_DEPRECATED(msg) WIRESTEAD_DEPRECATED_MSG(msg)
#endif
