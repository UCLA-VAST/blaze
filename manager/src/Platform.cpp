#include <glog/logging.h>

#include "TaskEnv.h"
#include "QueueManager.h"
#include "Platform.h"

namespace blaze {

Platform::Platform() {
  env = new TaskEnv();
}

Platform::~Platform() {
  delete env;  
}

// store an accelerator setup on the platform
void Platform::setupAcc(AccWorker &conf) {
  if (acc_table.find(conf.id()) == acc_table.end()) {
    acc_table.insert(std::make_pair(conf.id(), conf));
  }
}

// create a platform-specific queue manager
QueueManager_ptr Platform::createQueue() {
  QueueManager_ptr queue(new QueueManager(this));
  return queue;
}

// create a block object for the specific platform
DataBlock_ptr Platform::createBlock(
    int num_items, int item_length, int item_size, 
    int align_width) 
{
  return env->createBlock(num_items, item_length, 
      item_size, align_width);
}

// get an entry in the config_table matching the key
std::string Platform::getConfig(std::string &key) {
  if (config_table.find(key)==config_table.end()) {
    return std::string();
  } else {
    return config_table[key];
  }
}

// get TaskEnv to pass to Task
TaskEnv* Platform::getEnv() {
  return env;
}
} // namespace blaze
