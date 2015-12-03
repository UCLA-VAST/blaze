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

  virtual QueueManager_ptr createQueue();

  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0);

  int getNumDevices();

  virtual TaskEnv_ptr getEnv(std::string id);

  OpenCLEnv* getEnv(int device_id);

  virtual void setupAcc(AccWorker &con);

private:
  int load_file(const char* filename, char** result);
  
  uint32_t     num_devices;
  cl_context   context;
  cl_device_id device_id;  // only for clProgramBuildInfo

  std::vector<OpenCLEnv*> env_list; 
  std::map<std::string, cl_program> programs;

  //std::map<std::string, std::pair<int, char*> > bitstreams;
  //std::map<std::string, cl_kernel>  kernels;
};

extern "C" Platform* create();

extern "C" void destroy(Platform* p);

} // namespace blaze

#endif
