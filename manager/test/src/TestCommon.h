#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include <unistd.h>
#include <dlfcn.h>

#include <cstdint>
#include <string>
#include <stdexcept>

#include <boost/thread/thread.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "Common.h"

namespace blaze {

// custom exceptions
class cpuCalled : public std::runtime_error {
public:
  explicit cpuCalled(const std::string& what_arg):
    std::runtime_error(what_arg) {;}
};

class BlockTests : public ::testing::Test {
  protected:
    BlockTests(); 
    virtual void SetUp();
    virtual void TearDown();

    Platform_ptr  platform;
    BlockManager* bman;
};

class ClientTests : public ::testing::Test {
  protected:
    ClientTests() { }
};
}

#endif
