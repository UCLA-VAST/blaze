#ifndef OPENCL_PLATFORM_H
#define OPENCL_PLATFORM_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>

#include <CL/opencl.h>

#include "Platform.h"
#include "OpenCLBlock.h"
#include "OpenCLEnv.h"

namespace blaze {

class QueueManager;

typedef boost::shared_ptr<QueueManager> QueueManager_ptr;

class OpenCLPlatform : public Platform {

public:

  OpenCLPlatform();

  ~OpenCLPlatform() {
    delete env;  
    /*
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
    */


    clReleaseCommandQueue(cmd_queue);
    clReleaseContext(context);

  }

  virtual QueueManager_ptr createQueue();

  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0) 
  {
    DataBlock_ptr block(
        new OpenCLBlock((OpenCLEnv*)env,
          num_items, item_length, item_size, align_width)
        );  
    return block;
  }

  void setupProgram(std::string acc_id);

private:

  int load_file(const char* filename, char** result);
  
  std::string curr_acc_id;

  cl_device_id     device_id;
  cl_context       context;
  cl_command_queue cmd_queue;

  cl_program       prev_program;
  cl_kernel        prev_kernel;

  std::map<std::string, std::pair<int, unsigned char*> > bitstreams;
  //std::map<std::string, cl_kernel>  kernels;
};

extern "C" Platform* create();

extern "C" void destroy(Platform* p);

} // namespace blaze

#endif
