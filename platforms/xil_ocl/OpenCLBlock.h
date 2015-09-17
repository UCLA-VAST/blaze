#ifndef OPENCLBLOCK_H
#define OPENCLBLOCK_H

#include <stdio.h>
#include <string.h>
#include <string>
#include <stdexcept>

#include <boost/smart_ptr.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

#include "blaze.h"
#include "OpenCLEnv.h"

namespace blaze {

class OpenCLBlock : public DataBlock 
{

public:
  // create a single output elements
  OpenCLBlock(OpenCLEnv* _env, size_t _length, size_t _size):
    env(_env)
  {
    length = _length;
    num_items = 1;
    size = _size;
    ready = false;

    cl_context context = env->getContext();

    cl_int err = 0;

    data = clCreateBuffer(
        context, CL_MEM_READ_ONLY,  
        _size, NULL, &err);

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to create OpenCL block");
    }

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

  virtual void alloc(int64_t _size);

  // copy data from an array
  virtual void writeData(void* src, size_t _size);

  // copy data from an array with offset
  virtual void writeData(void* src, size_t _size, size_t offset);

  // write data to an array
  virtual void readData(void* dst, size_t size);

  // sample the items in the block by a mask
  virtual DataBlock_ptr sample(char* mask);

  virtual char* getData() { 

    if (allocated) {
      // this is a reinterpretive cast from cl_mem* to char*
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
