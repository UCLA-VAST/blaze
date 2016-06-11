#ifndef OPENCLENV_H
#define OPENCLENV_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>

#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <CL/opencl.h>

#include "blaze/TaskEnv.h"
#include "OpenCLCommon.h"

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
  friend class OpenCLQueueManager;

public:
  OpenCLTaskEnv(
      std::vector<OpenCLEnv*>& _env_list, 
      std::vector<cl_program>& _program_list):
    env_list(_env_list), 
    program_list(_program_list),
    location(-1)
  { 
    num_devices = _env_list.size();
  }

  cl_context getContext() {
    if (location < 0) {
      return 0;
    } else {
      return env_list[location]->getContext();
    }
  }
  cl_command_queue getCmdQueue() {
    if (location < 0) {
      return 0;
    } else {
      return env_list[location]->getCmdQueue();
    }
  }
  cl_program getProgram() {
    if (location < 0) {
      return 0;
    } else {
      return program_list[location];
    }
  }
  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0, 
      int flag = BLAZE_OUTPUT_BLOCK);

  virtual DataBlock_ptr createBlock(const OpenCLBlock& block);

  void relocate(int loc);

private:
  std::vector<OpenCLEnv*> &env_list;
  std::vector<cl_program> &program_list;

  // index of device where the block is allocated
  int location;

  int num_devices;

};
}

#endif 
