#ifndef OPENCLBLOCK_H
#define OPENCLBLOCK_H

#include <stdio.h>
#include <string.h>
#include <string>
#include <stdexcept>

#include <boost/smart_ptr.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

#include "blaze/Block.h"
#include "OpenCLCommon.h"
#include "OpenCLEnv.h"

namespace blaze {

class OpenCLBlock : public DataBlock 
{
  friend class OpenCLQueueManager;

public:
  // create a single output elements
  OpenCLBlock(
      std::vector<OpenCLEnv*> &_env_list, 
      int _num_items, 
      int _item_length,
      int _item_size,
      int _align_width = 0, 
      int _flag = BLAZE_INPUT_BLOCK);
 
  OpenCLBlock(const OpenCLBlock& block);

  ~OpenCLBlock();

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

  virtual char* getData();

  // relocate the block to another device memory
  void relocate(int loc);

  int getDeviceId();

private:
  cl_mem* data;

  // index of device where the block is allocated
  int location;

  int num_devices;

  std::vector<OpenCLEnv*> &env_list;
};
}

#endif
