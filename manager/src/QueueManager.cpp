#include <fstream>
#include <stdexcept>
#include <dlfcn.h>

#include <glog/logging.h>
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
      new TaskManager(create_func, destroy_func, platform));

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
    TaskManager_ptr task_manager = iter->second;
    task_manager->start();
  }
}

} // namespace blaze

