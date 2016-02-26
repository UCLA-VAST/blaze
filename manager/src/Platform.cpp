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

// Start TaskQueues for the CPU platform
// all the task queues can have simultaneous executors
void Platform::addQueue(AccWorker &conf) {

  this->setupAcc(conf); // DEBUG: does it call extended func?

  // add a TaskManager, and the scheduler should be started
  queue_manager->add(conf.id(), conf.path());

  // start a corresponding executor
  queue_manager->start(conf.id());
}

void Platform::removeQueue(std::string id) {

  // asynchronously call queue_manager->remove(id)
  boost::thread executor(
      boost::bind(&QueueManager::remove, queue_manager.get(), id));
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

TaskManager_ref Platform::getTaskManager(std::string acc_id) {
  TaskManager_ref ret = queue_manager->get(acc_id);
  return ret;
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
