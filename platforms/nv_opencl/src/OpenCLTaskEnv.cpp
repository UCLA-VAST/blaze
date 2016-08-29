#include <glog/logging.h>
#include "blaze/nv_opencl/OpenCLBlock.h"
#include "blaze/nv_opencl/OpenCLEnv.h" 

namespace blaze {
  
DataBlock_ptr OpenCLTaskEnv::createBlock(
      int num_items, int item_length,
      int item_size, int align_width, 
      int flag) 
{
  OpenCLBlock* new_block = new OpenCLBlock(env_list,
        num_items, item_length, item_size, align_width, flag);

  new_block->relocate(location);

  DataBlock_ptr block(new_block);  

  return block;
}

DataBlock_ptr OpenCLTaskEnv::createBlock(
    const OpenCLBlock& block)
{
  DataBlock_ptr bp(
      new OpenCLBlock(block));
  return bp;
}


void OpenCLTaskEnv::relocate(int loc) {
  location = loc;
}

}
