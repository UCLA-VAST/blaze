#ifndef TASK_MANAGER_H
#define TASK_MANAGER_H

#include <map>
#include <vector>
#include <iostream>

#include <boost/smart_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include "TaskEnv.h"
#include "Task.h"
#include "TaskQueue.h"
#include "Block.h"
#include "Logger.h"

namespace blaze {

/**
 * Manages a task queue for one accelerator executor
 */
class TaskManager 
: public boost::basic_lockable_adapter<boost::mutex>
{

public:

  TaskManager(
    Task* (*create_func)(), 
    void (*destroy_func)(Task*),
    Platform *_platform, 
    Logger *_logger
  ): power(true),  // TODO: 
     waitTime(0),
     deltaDelay(0),
     createTask(create_func),
     destroyTask(destroy_func),
     platform(_platform),
     logger(_logger)
  {
    ;
  }

  int estimateDelay(Task_ptr task);

  // create a task and return the task pointer
  Task_ptr create();

  // enqueue a task in the corresponding application queue
  void enqueue(std::string app_id, Task_ptr task);

  // schedule a task from app queues to execution queue  
  void schedule();

  // execute front task in the queue
  void execute();

  // get wait time for any task of current accelerator
  int getWaitTime() { return waitTime; }

  // start and stop executor and scheduler threads
  // TODO
  void start();
  //void stop();

  // experimental
  std::string getConfig(int idx, std::string key);

private:

  bool power;
  int waitTime;
  int deltaDelay;
  
  // Task implementation loaded from user acc_impl
  Task* (*createTask)();
  void (*destroyTask)(Task*);

  Platform *platform;
  Logger  *logger;

  void updateDelayModel(Task* task, int estimateTime, int realTime);

  //TODO: add a pointer to GAM communicator

  // application queues mapped by application id
  std::map<std::string, TaskQueue_ptr> app_queues;

  TaskQueue execution_queue;
};
}

#endif
