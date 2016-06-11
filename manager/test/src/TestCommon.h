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

class ConfigTests : public ::testing::Test {
  protected:
    ConfigTests() { }
};

// worker function to run app tests using Client
bool runArrayTest();
bool runLoopBack(int data_size = 1024);
bool runDelay(int data_size = 1024);

// global variables
static int app_port = 7777;
static std::string platform_dir = "../../platforms";
static std::string nv_opencl_path  = platform_dir + "/nv_opencl/nv_opencl.so";
static std::string lnx_opencl_path = platform_dir + "/xlnx_opencl/xlnx_opencl.so";
static std::string pathToArrayTest = "./tasks/cpu/arrayTest/arrayTest.so";
static std::string pathToLoopBack = "./tasks/cpu/loopBack/loopBack.so";

}

#endif
