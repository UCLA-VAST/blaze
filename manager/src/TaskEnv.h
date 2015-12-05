#ifndef TASKENV_H
#define TASKENV_H

#include <boost/smart_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include "Common.h"
#include "Block.h"

namespace blaze {

class TaskEnv {
public: 
  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0, 
      int flag = BLAZE_INPUT_BLOCK) 
  {
    DataBlock_ptr block(new DataBlock(
          num_items, item_length, item_size, align_width, flag));
    return block;
  }
};
}
#endif
