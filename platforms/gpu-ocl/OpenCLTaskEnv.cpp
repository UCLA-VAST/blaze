
#include "OpenCLEnv.h" 
#include "OpenCLBlock.h"

namespace blaze {
  
DataBlock_ptr OpenCLTaskEnv::createBlock(
      int num_items, int item_length,
      int item_size, int align_width, 
      int flag) 
{
  DataBlock_ptr block(
      new OpenCLBlock(env,
        num_items, item_length, item_size, align_width, flag)
      );  
  return block;
}

}
