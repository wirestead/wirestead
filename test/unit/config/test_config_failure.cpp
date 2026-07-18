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

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <vector>

#include "test_utils.hpp"
#include "wirestead/config/config_manager.hpp"

using namespace wirestead::config;
using namespace wirestead::test;

namespace {

class ConfigFailureTest : public ::testing::Test {
 protected:
  void SetUp() override { manager_ = std::make_unique<ConfigManager>(); }

  void TearDown() override {
    // Cleanup files
    for (const auto& path : temp_files_) {
      TestUtils::removeFileIfExists(path);
    }
  }

  std::filesystem::path create_temp_file(const std::string& name, const std::string& content) {
    auto path = TestUtils::makeTempFilePath(name);
    std::ofstream ofs(path);
    ofs << content;
    ofs.close();
    temp_files_.push_back(path);
    return path;
  }

  std::unique_ptr<ConfigManager> manager_;
  std::vector<std::filesystem::path> temp_files_;
};

TEST_F(ConfigFailureTest, MalformedInput) {
  // ConfigManager expects "key=value"
  // We give it garbage lines
  auto path = create_temp_file("malformed.conf", "key_without_value\n=value_without_key\n   \n#comment\nkey=value");

  // It should skip invalid lines and parse valid ones
  EXPECT_TRUE(manager_->load_from_file(path.string()));

  // Verify valid key was loaded
  EXPECT_TRUE(manager_->has("key"));
  EXPECT_EQ(std::any_cast<std::string>(manager_->get("key")), "value");
}

TEST_F(ConfigFailureTest, TypeMismatch) {
  // Pre-register an integer key
  ConfigItem item("int_key", 0, ConfigType::Integer, false);
  manager_->register_item(item);

  // Create file with string value for int key
  auto path = create_temp_file("mismatch.conf", "int_key=not_an_integer");

  // Load should succeed (return true) but skip/log error for the invalid key
  EXPECT_TRUE(manager_->load_from_file(path.string()));

  // Value should remain default (0)
  EXPECT_EQ(std::any_cast<int>(manager_->get("int_key")), 0);
}

TEST_F(ConfigFailureTest, MissingFile) {
  auto path = TestUtils::makeTempFilePath("non_existent.conf");
  TestUtils::removeFileIfExists(path);  // Ensure it doesn't exist

  EXPECT_FALSE(manager_->load_from_file(path.string()));
}

}  // namespace
