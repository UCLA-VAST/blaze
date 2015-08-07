#include "OpenCLBlock.h"

namespace acc_runtime {

#define LOG_HEADER  std::string("OpenCLBlock::") + \
                    std::string(__func__) +\
                    std::string("(): ")

void OpenCLBlock::alloc(int64_t _size) {

  //boost::lock_guard<OpenCLBlock> guard(*this);

  if (!allocated) {
    cl_context context = env->getContext();
    cl_int err = 0;

    data = clCreateBuffer(
        context, CL_MEM_READ_ONLY,  
        _size, NULL, &err);

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to create OpenCL block");
    }

    size = _size;

    allocated = true;
  }
}

void OpenCLBlock::writeData(void* src, size_t _size) {

  // NOTE: two locks in the same function, is it possible
  // for deadlocks?
  //boost::lock_guard<OpenCLBlock> guard(*this);

  if (allocated) {

    // WriteBuffer need to be exclusive
    // lock env for this 
    boost::lock_guard<OpenCLEnv> guard(*env);

    cl_command_queue command = env->getCmdQueue();
    cl_event event;

    int err = clEnqueueWriteBuffer(
      command, data, CL_TRUE, 0, 
      _size, src, 0, NULL, &event);

    if (err != CL_SUCCESS) {
      throw std::runtime_error(
          "Failed to write to OpenCL block"+
          std::to_string((long long)err));
    }
    clWaitForEvents(1, &event);

    ready = true;
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}

void OpenCLBlock::writeData(void* src, size_t _size, size_t offset) {

  // NOTE: two locks in the same function, is it possible
  // for deadlocks?
  //boost::lock_guard<OpenCLBlock> guard(*this);

  if (allocated) {
    if (offset+_size > size) {
      throw std::runtime_error("Exists block size");
    }

    // WriteBuffer need to be exclusive
    // lock env for this 
    boost::lock_guard<OpenCLEnv> guard(*env);

    // get the command queue handler
    cl_command_queue command = env->getCmdQueue();
    cl_event event;

    int err = clEnqueueWriteBuffer(
      command, data, CL_TRUE, offset, 
      _size, src, 0, NULL, &event);

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to write to OpenCL block");
    }
    clWaitForEvents(1, &event);

    if (offset + _size == size) {
      ready = true;
    }
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}

// write data to an array
void OpenCLBlock::readData(void* dst, size_t size) {
  if (allocated) {

    // ReadBuffer need to be exclusive
    // lock env for this 
    boost::lock_guard<OpenCLEnv> guard(*env);
    
    // get the command queue handler
    cl_command_queue command = env->getCmdQueue();
    cl_event event;

    int err = clEnqueueReadBuffer(
      command, data, CL_TRUE, 0, 
      size, dst, 0, NULL, &event);

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to write to OpenCL block");
    }
    clWaitForEvents(1, &event);
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}

} // namespace

