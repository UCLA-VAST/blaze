#ifndef OPENCL_PLATFORM_H
#define OPENCL_PLATFORM_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>

#include <CL/opencl.h>

#include "proto/acc_conf.pb.h"
#include "Platform.h"
#include "OpenCLBlock.h"
#include "OpenCLEnv.h"

namespace blaze {

class OpenCLPlatform : public Platform {

public:

  OpenCLPlatform();

  virtual void setupAcc(AccWorker &conf);

  ~OpenCLPlatform() {
    delete env;  
    for (std::map<std::string, cl_program>::iterator 
             iter = programs.begin(); 
           iter != programs.end(); 
           iter ++) 
      {
        clReleaseProgram(iter->second);
      }

      for (std::map<std::string, cl_kernel>::iterator 
             iter = kernels.begin(); 
           iter != kernels.end(); 
           iter ++) 
      {
        clReleaseKernel(iter->second);
      }
      

    clReleaseCommandQueue(cmd_queue);
    clReleaseContext(context);

  }

  virtual DataBlock_ptr createBlock() {
    DataBlock_ptr block(
        new OpenCLBlock(dynamic_cast<OpenCLEnv*>(env)));  
    return block;
  }

  virtual DataBlock_ptr createBlock(size_t length, size_t size) {
    DataBlock_ptr block(
        new OpenCLBlock(dynamic_cast<OpenCLEnv*>(env), length, size));  
    return block;
  }

private:

  int load_file(const char* filename, char** result);
  
  std::string curr_acc_id;

  cl_device_id     device_id;
  cl_context       context;
  cl_command_queue cmd_queue;

  std::map<std::string, cl_program> programs;
  std::map<std::string, cl_kernel>  kernels;
};

extern "C" Platform* create() {
  return new OpenCLPlatform();
}

extern "C" void destroy(Platform* p) {
  delete p;
}
} // namespace blaze

#endif
