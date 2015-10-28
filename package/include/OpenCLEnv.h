#ifndef OPENCLENV_H
#define OPENCLENV_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>

#include <CL/opencl.h>

#include "TaskEnv.h"

namespace blaze {

class OpenCLPlatform;

class OpenCLEnv : public TaskEnv 
{
  friend class OpenCLPlatform;

public:
  
  OpenCLEnv(
      cl_context _context,
      cl_command_queue _queue
    ): TaskEnv(), context(_context), cmd_queue(_queue)
  { ;}

  // TODO: potentially this part needs to be exclusive 
  cl_context& getContext() { return context; }

  cl_command_queue& getCmdQueue() { return cmd_queue; }

  cl_kernel& getKernel() { return kernel; }

private:

  void changeKernel(cl_kernel &new_kernel) {
    kernel = new_kernel;   
  }
 
  cl_context       context;
  cl_command_queue cmd_queue;
  cl_kernel        kernel;
};
}

#endif 
