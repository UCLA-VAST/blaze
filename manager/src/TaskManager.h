#ifndef TASK_MANAGER_H
#define TASK_MANAGER_H

#include <map>
#include <vector>
#include <iostream>

#include <boost/smart_ptr.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include "TaskEnv.h"
#include "Task.h"
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
  ): length(0),
     createTask(create_func),
     destroyTask(destroy_func),
     platform(_platform),
     logger(_logger)
  {
    ;
  }

  // create a task and return the task pointer
  Task* create();

  // execute front task in the queue
  void execute();

  // retire tasks that are committed from the retire queue
  void commit();

  // experimental
  std::string getConfig(int idx, std::string key);

private:

  // NOTE: experiments
  int length;
  
  Task* (*createTask)();
  void (*destroyTask)(Task*);

  Platform *platform;
  Logger  *logger;

  boost::lockfree::queue<Task*, boost::lockfree::capacity<1024> > task_queue;
  boost::lockfree::queue<Task*, boost::lockfree::capacity<1024> > retire_queue;
};
}

#endif
