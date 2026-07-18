#include <gtest/gtest.h>

#include <any>
#include <cstdio>
#include <fstream>

#include "wirestead/config/config_manager.hpp"

namespace wirestead {
namespace config {
namespace test {

TEST(ConfigSecurityTest, LoadFromFile_TypeConfusion) {
  ConfigManager manager;

  // Register an integer configuration item
  ConfigItem int_item("max_connections", 100, ConfigType::Integer);
  manager.register_item(int_item);

  // Verify initial state
  EXPECT_EQ(manager.get_type("max_connections"), ConfigType::Integer);
  EXPECT_EQ(std::any_cast<int>(manager.get("max_connections")), 100);

  // Create a malicious config file that sets this integer to a boolean
  std::string filename = "malicious_config.txt";
  std::ofstream out(filename);
  out << "max_connections=true" << std::endl;
  out.close();

  // Load the configuration
  // It should return true (as file exists) but log an error internally and NOT update the value
  bool load_result = manager.load_from_file(filename);
  EXPECT_TRUE(load_result);

  // Clean up
  std::remove(filename.c_str());

  // Verify that the type has NOT changed and value is preserved

  auto value = manager.get("max_connections");

  EXPECT_EQ(value.type(), typeid(int)) << "Type should remain Integer";
  if (value.type() == typeid(int)) {
    EXPECT_EQ(std::any_cast<int>(value), 100) << "Value should not be updated with invalid type";
  }
}

}  // namespace test
}  // namespace config
}  // namespace wirestead
