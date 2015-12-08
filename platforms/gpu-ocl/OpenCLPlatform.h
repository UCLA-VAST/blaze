#ifndef OPENCL_PLATFORM_H
#define OPENCL_PLATFORM_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>

#include <CL/opencl.h>

#include "OpenCLCommon.h"
#include "Platform.h"

namespace blaze {

class OpenCLPlatform : public Platform {

public:

  OpenCLPlatform();
  ~OpenCLPlatform();

  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0,
      int flag = BLAZE_INPUT_BLOCK);

  int getNumDevices();

  virtual TaskEnv_ptr getEnv(std::string id);

  OpenCLEnv* getEnv(int device_id);

  virtual void setupAcc(AccWorker &con);

  virtual void createBlockManager(size_t cache_limit, size_t scratch_limit);

  virtual BlockManager* getBlockManager();

  virtual void remove(int64_t block_id);

private:
  int load_file(const char* filename, char** result);
  
  uint32_t     num_devices;

  std::vector<OpenCLEnv*> env_list; 

  std::map<std::string, std::vector<cl_program> > program_list;

  std::vector<BlockManager_ptr> block_manager_list;
};

extern "C" Platform* create();

extern "C" void destroy(Platform* p);

} // namespace blaze

#endif
