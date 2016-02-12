#define LOG_HEADER "Platform"
#include <glog/logging.h>

#include "TaskEnv.h"
#include "TaskManager.h"
#include "BlockManager.h"
#include "QueueManager.h"
#include "Platform.h"

namespace blaze {

Platform::Platform(std::map<std::string, std::string> &conf_table)
{
  TaskEnv_ptr env_ptr(new TaskEnv());
  env = env_ptr;

  // create queue
  QueueManager_ptr queue(new QueueManager(this));
  queue_manager = queue;

}

// store an accelerator setup on the platform
void Platform::setupAcc(AccWorker &conf) {
  if (acc_table.find(conf.id()) == acc_table.end()) {
    acc_table.insert(std::make_pair(conf.id(), conf));
  }
}

// create a block object for the specific platform
DataBlock_ptr Platform::createBlock(
    int num_items, int item_length, int item_size, 
    int align_width, int flag) 
{
  return env->createBlock(num_items, item_length, 
      item_size, align_width, flag);
}

DataBlock_ptr Platform::createBlock(const DataBlock& block) 
{
  return env->createBlock(block);
}

// remove a shard block from the block manager
void Platform::remove(int64_t block_id) {
  block_manager->remove(block_id); 
}

void Platform::createBlockManager(size_t cache_limit, size_t scratch_limit) {
  BlockManager_ptr bman(new BlockManager(this, cache_limit, scratch_limit));
  block_manager = bman;
}

QueueManager* Platform::getQueueManager() {
  if (queue_manager) {
    return queue_manager.get();
  } else {
    return NULL;
  }
}

BlockManager* Platform::getBlockManager() {
  if (block_manager) {
    return block_manager.get();
  } else {
    return NULL;
  }
}

TaskManager* Platform::getTaskManager(std::string acc_id) {
  if (queue_manager && 
      queue_manager->get(acc_id)) 
  {
    return queue_manager->get(acc_id).get();
  } else {
    return NULL;
  }
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
TaskEnv_ptr Platform::getEnv(std::string id) {
  return env;
}
} // namespace blaze
