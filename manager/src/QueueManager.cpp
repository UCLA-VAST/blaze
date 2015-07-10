#include <stdexcept>
#include <dlfcn.h>

#include "QueueManager.h"

namespace acc_runtime {

#define LOG_HEADER  std::string("QueueManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")


void QueueManager::add(std::string id, std::string lib_path) {
  
  void* handle = dlopen(lib_path.c_str(), RTLD_LAZY);

  if (handle == NULL) {
    logger->logErr(LOG_HEADER + dlerror());
    throw std::runtime_error(dlerror());
  }
  // reset errors
  dlerror();

  // load the symbols
  Task* (*create_func)();
  void (*destroy_func)(Task*);

  create_func = (Task* (*)())dlsym(handle, "create");
  destroy_func = (void (*)(Task*))dlsym(handle, "destroy");

  const char* error = dlerror();
  if (error) {
    logger->logErr(LOG_HEADER + error);
    throw std::runtime_error(error);
  }

  TaskManager_ptr taskManager(
    new TaskManager(create_func, destroy_func, logger));

  queue_table.insert(std::make_pair(id, taskManager));
}

TaskManager_ptr QueueManager::get(std::string id) {

  if (queue_table.find(id) == queue_table.end()) {
    return NULL_TASK_MANAGER;   
  }
  else {
    return queue_table[id];
  }
}

} // namespace acc_runtime

