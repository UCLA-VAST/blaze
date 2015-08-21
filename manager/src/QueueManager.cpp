
#include <fstream>
#include <stdexcept>
#include <dlfcn.h>

#include "QueueManager.h"

namespace blaze {

#define LOG_HEADER  std::string("QueueManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")

void QueueManager::add(
    std::string id, 
    std::string lib_path,
    TaskEnv *env) 
{

  void* handle = dlopen(lib_path.c_str(), RTLD_LAZY|RTLD_GLOBAL);

  if (handle == NULL) {
    logger->logErr(LOG_HEADER + dlerror());
    throw std::runtime_error(dlerror());
  }
  // reset errors
  dlerror();

  // load the symbols
  Task* (*create_func)(TaskEnv*);
  void (*destroy_func)(Task*);

  // read the custom constructor and destructor  
  create_func = (Task* (*)(TaskEnv*))dlsym(handle, "create");
  destroy_func = (void (*)(Task*))dlsym(handle, "destroy");

  const char* error = dlerror();
  if (error) {
    logger->logErr(LOG_HEADER + error);
    throw std::runtime_error(error);
  }

  // construct the corresponding task queue
  TaskManager_ptr taskManager(
      new TaskManager(create_func, destroy_func, env, logger));

  queue_table.insert(std::make_pair(id, taskManager));

  logger->logInfo(
      LOG_HEADER +
      "added a new task queue: "+
      id);
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

  // start executor and commitor
  boost::thread executor(
      boost::bind(&TaskManager::execute, task_manager));

  boost::thread committer(
      boost::bind(&TaskManager::commit, task_manager));
}

void QueueManager::startAll() {
  std::map<std::string, TaskManager_ptr>::iterator iter;
  for (iter = queue_table.begin();
      iter != queue_table.end();
      ++iter)
  {
    TaskManager_ptr task_manager = iter->second;

    // start executor and commitor
    boost::thread executor(
        boost::bind(&TaskManager::execute, task_manager));

    boost::thread committer(
        boost::bind(&TaskManager::commit, task_manager));
  }
}

} // namespace blaze

