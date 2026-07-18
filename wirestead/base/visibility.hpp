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

#if __has_include(<wirestead_export.hpp>)
#include <wirestead_export.hpp>
#endif

#if !defined(WIRESTEAD_API)
#if defined(WIRESTEAD_BUILD_SHARED)
#if defined(_WIN32) || defined(__CYGWIN__)
#if defined(WIRESTEAD_BUILDING_LIBRARY)
#define WIRESTEAD_API __declspec(dllexport)
#else
#define WIRESTEAD_API __declspec(dllimport)
#endif
#else
#define WIRESTEAD_API __attribute__((visibility("default")))
#endif
#elif defined(WIRESTEAD_EXPORT)
#define WIRESTEAD_API WIRESTEAD_EXPORT
#else
#define WIRESTEAD_API
#endif
#endif

#if !defined(WIRESTEAD_LOCAL)
#if defined(WIRESTEAD_NO_EXPORT)
#define WIRESTEAD_LOCAL WIRESTEAD_NO_EXPORT
#elif defined(_WIN32) || defined(__CYGWIN__)
#define WIRESTEAD_LOCAL
#else
#define WIRESTEAD_LOCAL __attribute__((visibility("hidden")))
#endif
#endif

#ifndef WIRESTEAD_EXPORT
#define WIRESTEAD_EXPORT WIRESTEAD_API
#endif

#ifndef WIRESTEAD_NO_EXPORT
#define WIRESTEAD_NO_EXPORT WIRESTEAD_LOCAL
#endif

// Legacy UniLink export macros are source compatibility aliases for v0.9.x.
#ifndef UNILINK_API
#define UNILINK_API WIRESTEAD_API
#endif

#ifndef UNILINK_LOCAL
#define UNILINK_LOCAL WIRESTEAD_LOCAL
#endif

#ifndef UNILINK_EXPORT
#define UNILINK_EXPORT WIRESTEAD_EXPORT
#endif

#ifndef UNILINK_NO_EXPORT
#define UNILINK_NO_EXPORT WIRESTEAD_NO_EXPORT
#endif
