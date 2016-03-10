#ifndef OPENCL_PLATFORM_H
#define OPENCL_PLATFORM_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>

#include <CL/opencl.h>

#include "OpenCLCommon.h"
#include "Platform.h"
#include "OpenCLBlock.h"
#include "OpenCLEnv.h"

namespace blaze {

class OpenCLPlatform : public Platform {

public:

  OpenCLPlatform(std::map<std::string, std::string> &conf_table);

  ~OpenCLPlatform();

  virtual TaskEnv_ptr getEnv(std::string id);

  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0,
      int flag = BLAZE_INPUT_BLOCK);

  virtual void createBlockManager(size_t cache_limit, size_t scratch_limit);
  virtual BlockManager* getBlockManager();

  void addQueue(AccWorker &conf);
  void removeQueue(std::string id);

  void changeProgram(std::string acc_id);

  cl_kernel& getKernel();

private:

  int load_file(const char* filename, char** result);
  
  OpenCLEnv*  env;
  TaskEnv_ptr env_ptr;

  std::string curr_acc_id;
  cl_program  curr_program;
  cl_kernel   curr_kernel;

  std::map<std::string, std::pair<int, unsigned char*> > bitstreams;
  std::map<std::string, std::string> kernel_list;
  //std::map<std::string, cl_kernel>  kernels;
};

extern "C" Platform* create(
    std::map<std::string, std::string> &config_table);

extern "C" void destroy(Platform* p);

} // namespace blaze

#endif
