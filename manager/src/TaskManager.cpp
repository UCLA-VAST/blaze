#include <boost/date_time/posix_time/posix_time.hpp>

#include "TaskManager.h"

namespace acc_runtime {
#define LOG_HEADER  std::string("TaskManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")

Task* TaskManager::create() {
  
  Task* task = (Task*)createTask();

  task_queue.push(task);

  logger->logInfo(LOG_HEADER + std::string("created a new task"));

  return task;
}

void TaskManager::execute() {
  
  logger->logInfo(LOG_HEADER + std::string("started an executor"));

  // continuously execute tasks from the task queue
  while (1) { 

    // wait if there is no task to be executed
    while (task_queue.empty()) {
      boost::this_thread::sleep_for(boost::chrono::milliseconds(10)); 
    }

    // get next task and remove it from the task queue
    // this part is thread-safe with boost::lockfree::queue
    Task* task;
    task_queue.pop(task);

    // polling the status, wait for data to be transferred
    while (task->status != Task::READY) {
      boost::this_thread::sleep_for(boost::chrono::microseconds(10)); 
    }

    // start execution
    task->execute();

    // TODO: retry if task failed

    // put task into the retire queue
    retire_queue.push(task);

    logger->logInfo(LOG_HEADER + std::string("finished a task"));
  }
}

void TaskManager::commit() {
    
  logger->logInfo(LOG_HEADER + std::string("started an committer"));
  // continuously retiring tasks from the retire queue
  while (1) {

    // wait if there is no task to be retired
    while (retire_queue.empty()) {
      boost::this_thread::sleep_for(boost::chrono::milliseconds(10)); 
    }

    // get the next task popped from the retire queue
    Task* task;
    retire_queue.pop(task);

    // polling the status, wait for output data to be read
    while (task->status != Task::COMMITTED) {
      boost::this_thread::sleep_for(boost::chrono::microseconds(10)); 
    }
    
    // deallocate the object using the function loaded from library
    destroyTask(task);

    logger->logInfo(LOG_HEADER + std::string("retired a task"));
  }
}

} // namespace acc_runtime
