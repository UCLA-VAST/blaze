#include <fstream>
#include <stdexcept>
#include <dlfcn.h>

#define LOG_HEADER "QueueManager"

#include <boost/smart_ptr.hpp>
#include <boost/filesystem.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include <glog/logging.h>

#include "Task.h"
#include "Block.h"
#include "Platform.h"
#include "TaskManager.h"
#include "QueueManager.h"

namespace blaze {

void QueueManager::add(
    std::string id, 
    std::string lib_path)
{
  void* handle = dlopen(lib_path.c_str(), RTLD_LAZY|RTLD_GLOBAL);

  if (handle == NULL) {
    throw std::runtime_error(dlerror());
  }
  // reset errors
  dlerror();

  // load the symbols
  Task* (*create_func)();
  void (*destroy_func)(Task*);

  // read the custom constructor and destructor  
  create_func = (Task* (*)())dlsym(handle, "create");
  destroy_func = (void (*)(Task*))dlsym(handle, "destroy");

  const char* error = dlerror();
  if (error) {
    throw std::runtime_error(error);
  }

  // construct the corresponding task queue
  TaskManager_ptr taskManager(
      new TaskManager(create_func, destroy_func, id, platform));

  queue_table.insert(std::make_pair(id, taskManager));

  LOG(INFO) << "added a new task queue: " << id;
}

TaskManager_ptr QueueManager::get(std::string id) {

  if (queue_table.find(id) == queue_table.end()) {
    return NULL_TASK_MANAGER;   
  }
  else {
    return queue_table[id];
  }
}

TaskEnv* QueueManager::getTaskEnv(Task* task) {
  return task->getEnv();
}

void QueueManager::setTaskEnv(Task* task, TaskEnv_ptr env) {
  task->env = env;
}

DataBlock_ptr QueueManager::getTaskInputBlock(Task *task, int idx) {
  if (idx < task->input_blocks.size() &&
      task->input_table.find(task->input_blocks[idx]) 
        != task->input_table.end())
  {
    return task->input_table[task->input_blocks[idx]];
  } else {
    return NULL_DATA_BLOCK; 
  }
}

void QueueManager::setTaskInputBlock(
    Task *task, 
    DataBlock_ptr block, 
    int idx) 
{
  if (idx < task->input_blocks.size()) {
    int64_t block_id = task->input_blocks[idx];
    DLOG(INFO) << "Reset task input block " << block_id;
    task->inputBlockReady(block_id, block);
  }
}

// Start TaskQueues for the CPU platform
// all the task queues can have simultaneous executors
void QueueManager::start(std::string id) {

  // get the reference to the task queue 
  TaskManager_ptr task_manager = get(id);

  if (task_manager == NULL_TASK_MANAGER) {
    throw std::runtime_error("No matching task queue");
    return;
  }
  task_manager->start();
}

void QueueManager::startAll() {
  std::map<std::string, TaskManager_ptr>::iterator iter;
  for (iter = queue_table.begin();
      iter != queue_table.end();
      ++iter)
  {
    start(iter->first);
  }
}

} // namespace blaze

