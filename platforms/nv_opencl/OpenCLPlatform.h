#ifndef OPENCL_PLATFORM_H
#define OPENCL_PLATFORM_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>

#include <CL/opencl.h>

#include "blaze/Platform.h"
#include "OpenCLCommon.h"

namespace blaze {

class OpenCLPlatform : public Platform {

public:

  OpenCLPlatform(std::map<std::string, std::string> &conf_table);

  ~OpenCLPlatform();

  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0,
      int flag = BLAZE_INPUT_BLOCK);

  virtual DataBlock_ptr createBlock(
      const OpenCLBlock& block);

  int getNumDevices();

  virtual TaskEnv_ptr getEnv(std::string id);

  OpenCLEnv* getEnv(int device_id);

  void addQueue(AccWorker &conf);

private:
  int load_file(const char* filename, char** result);
  
  uint32_t num_devices;

  std::vector<OpenCLEnv*> env_list; 

  std::map<std::string, std::vector<cl_program> > program_list;

  std::vector<BlockManager_ptr> block_manager_list;
};

extern "C" Platform* create(
    std::map<std::string, std::string> &config_table);

extern "C" void destroy(Platform* p);

} // namespace blaze

#endif
