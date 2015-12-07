#ifndef OPENCLENV_H
#define OPENCLENV_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>

#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <CL/opencl.h>

#include "OpenCLCommon.h"
#include "TaskEnv.h"

namespace blaze {

class OpenCLEnv
: public boost::basic_lockable_adapter<boost::mutex>
{
public:
  OpenCLEnv(
      int _id,
      cl_context _context,
      cl_command_queue _queue,
      cl_device_id _device_id): 
    id(_id), 
    context(_context), 
    cmd_queue(_queue),
    device_id(_device_id)
  {;}

  int getDevice() { return id; }
  cl_device_id& getDeviceId() { return device_id; }
  cl_context& getContext() { return context; }
  cl_command_queue& getCmdQueue() { return cmd_queue; }

private:
  int id;
  cl_device_id     device_id;
  cl_context       context;
  cl_command_queue cmd_queue;
};

class OpenCLTaskEnv : public TaskEnv 
{
  friend OpenCLQueueManager;

public:
  OpenCLTaskEnv(OpenCLEnv* _env, cl_program _program):
    env(_env), program(_program) 
  {;}

  cl_context& getContext() {
    return env->getContext();
  }
  cl_command_queue& getCmdQueue() {
    return env->getCmdQueue();
  }
  cl_program& getProgram() {
    return program;
  }
  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0, 
      int flag = BLAZE_OUTPUT_BLOCK);

private:
  OpenCLEnv* env;

  cl_program program;
};
}

#endif 
