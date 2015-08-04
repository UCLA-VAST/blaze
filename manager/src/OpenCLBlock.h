#ifndef OPENCLBLOCK_H
#define OPENCLBLOCK_H

#include <string.h>
#include <string>
#include <stdexcept>

#include <boost/smart_ptr.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

#include "Block.h"
#include "OpenCLEnv.h"

namespace acc_runtime {

class OpenCLBlock : public DataBlock 
{

public:
  // create a single output elements
  OpenCLBlock(OpenCLEnv* _env, int _length, int _size):
    env(_env)
  {
    length = _length;
    num_items = 1;
    size = _size;
    ready = false;

    cl_context context = env->getContext();

    // TODO: exception handling
    data = clCreateBuffer(
        context, CL_MEM_READ_ONLY,  
        _size, NULL, NULL);

    allocated = true;
  }
  
  OpenCLBlock(OpenCLEnv* _env): DataBlock(), env(_env)
  {
    ;  
  }

  OpenCLBlock(OpenCLEnv* _env, DataBlock *block):
    env(_env) 
  {
    length = block->getLength();
    size = block->getSize();
    num_items = block->getNumItems();
    if (block->isAllocated()) {
         
      cl_context context = env->getContext();

      data = clCreateBuffer(
          context, CL_MEM_READ_ONLY,  
          size, NULL, NULL);

      allocated = true;
    }
    // if ready, copy the data over
    if (block->isReady()) {
      writeData((void*)block->getData(), size);
      ready = true;
    }
  }
  
  ~OpenCLBlock() {
    if (allocated) {
      clReleaseMemObject(data);
    }
  }

  virtual void alloc(int _size) {

    cl_context context = env->getContext();

    // TODO: exception handling
    data = clCreateBuffer(
        context, CL_MEM_READ_ONLY,  
        _size, NULL, NULL);

    size = _size;

    allocated = true;
  }

  // TODO: maybe enqueueBufferWrite/Read will not be thread safe,
  // then need to add mutex
  
  // copy data from an array
  virtual void writeData(void* src, size_t _size) {

    if (allocated) {

      cl_command_queue command = env->getCmdQueue();
      cl_event event;

      int err = clEnqueueWriteBuffer(
        command, data, CL_TRUE, 0, 
        _size, src, 0, NULL, &event);

      if (err != CL_SUCCESS) {
        throw std::runtime_error("Failed to write to OpenCL block");
      }
      clWaitForEvents(1, &event);

      ready = true;
    }
    else {
      throw std::runtime_error("Block memory not allocated");
    }
  }

  // copy data from an array with offset
  virtual void writeData(void* src, size_t _size, size_t offset) {
    if (allocated) {
      if (offset+_size > size) {
        throw std::runtime_error("Exists block size");
      }

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
  virtual void readData(void* dst, size_t size) {
    if (allocated) {

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

  // TODO: check if this reinterpretive cast is valid
  virtual char* getData() { 

    if (allocated) {
      return (char*)&data; 
    }
    else {
      return NULL;
    }
  }

private:
  cl_mem data;
  OpenCLEnv *env;

};
}

#endif
