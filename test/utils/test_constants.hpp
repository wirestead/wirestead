#pragma once

#include <chrono>

namespace wirestead {
namespace test {
namespace constants {

// Common timeout values for tests to avoid magic numbers
constexpr std::chrono::milliseconds kInstantTimeout{10};
constexpr std::chrono::milliseconds kShortTimeout{50};
constexpr std::chrono::milliseconds kDefaultTimeout{100};
constexpr std::chrono::milliseconds kMediumTimeout{200};
constexpr std::chrono::milliseconds kLongTimeout{500};
constexpr std::chrono::milliseconds kStressTimeout{2000};

// Retry intervals
constexpr std::chrono::milliseconds kRetryInterval{10};

}  // namespace constants
}  // namespace test
}  // namespace wirestead
