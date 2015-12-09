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
  OpenCLBlock(OpenCLEnv* _env, 
      int _num_items, 
      int _item_length,
      int _item_size,
      int _align_width = 0)
      :
    env(_env), 
    DataBlock(_num_items, _item_length, _item_size, _align_width)
  {
    ;
  }
  
  // used to copy data from CPU memory
  OpenCLBlock(OpenCLEnv* _env, DataBlock *block):
    env(_env),
    DataBlock(*block)
  {
    if (block->isAllocated()) {
      alloc(); 
    }
    // if ready, copy the data over
    if (block->isReady()) {
      DataBlock::writeData((void*)block->getData(), size);
      ready = true;
    }
  }
  
  ~OpenCLBlock() {
    if (allocated) {
      clReleaseMemObject(data);
    }
  }

  virtual void alloc();

  // read/write data from/to shared memory
  virtual void readFromMem(std::string path);
  virtual void writeToMem(std::string path);

  // copy data from an array
  virtual void writeData(void* src, size_t _size);
  virtual void writeData(void* src, size_t _size, size_t offset);

  // write data to an array
  virtual void readData(void* dst, size_t size);

  // sample the items in the block by a mask
  virtual DataBlock_ptr sample(char* mask);

  virtual char* getData() { 

    alloc();

    // this is a reinterpretive cast from cl_mem* to char*
    return (char*)&data; 
  }

private:
  cl_mem data;
  OpenCLEnv *env;

};
}

#endif
