#ifndef TASKENV_H
#define TASKENV_H

#include "Common.h"

namespace blaze {

class TaskEnv {
public: 
  virtual DataBlock_ptr createBlock(
      int num_items, int item_length, int item_size, 
      int align_width = 0, int flag = BLAZE_INPUT_BLOCK);

  virtual DataBlock_ptr createBlock(const DataBlock& block);
};
}
#endif
