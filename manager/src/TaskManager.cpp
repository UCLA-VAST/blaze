#include <boost/date_time/posix_time/posix_time.hpp>

#include "TaskManager.h"

namespace blaze {
#define LOG_HEADER  std::string("TaskManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")

Task* TaskManager::create() {
  
  // create a new task by the constructor loaded form user implementation
  Task* task = (Task*)createTask();

  // link the task platform 
  task->setPlatform(platform);

  // add task to queue lock-free
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
      boost::this_thread::sleep_for(boost::chrono::microseconds(100)); 
    }

    // get next task and remove it from the task queue
    // this part is thread-safe with boost::lockfree::queue
    Task* task;
    task_queue.pop(task);

    // polling the status, wait for data to be transferred
    while (!task->isReady()) {
      boost::this_thread::sleep_for(boost::chrono::microseconds(100)); 
    }
    logger->logInfo(LOG_HEADER + std::string("Started a new task"));

    try {
      // start execution
      task->execute();
    } 
    catch (std::runtime_error &e) {
      logger->logErr(LOG_HEADER+ 
          std::string("task->execute() error: ")+
          e.what());
    }

    if (task->status == Task::FINISHED)  {

      // put task into the retire queue
      retire_queue.push(task);

      logger->logInfo(LOG_HEADER + std::string("Task finished"));
    }
    else { // task failed, retry 

      // TODO: add a retry counter in task
      //task_queue.push(task);

      logger->logInfo(LOG_HEADER + std::string(
            "Task failed due to previous error"));
    }
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

} // namespace blaze
