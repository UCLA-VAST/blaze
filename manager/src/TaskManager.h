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
    Task* (*create_func)(TaskEnv*), 
    void (*destroy_func)(Task*),
    TaskEnv *_env, 
    Logger *_logger
  ): length(0),
     createTask(create_func),
     destroyTask(destroy_func),
     env(_env),
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

  //void onDataReady(int task_id, int partition_id);

  int getQueueLength() { return length;}
  int getWaitTime();

private:

  // NOTE: experiments
  int length;
  
  Task* (*createTask)(TaskEnv*);
  void (*destroyTask)(Task*);

  TaskEnv *env;
  Logger  *logger;

  boost::lockfree::queue<Task*, boost::lockfree::capacity<1024> > task_queue;
  boost::lockfree::queue<Task*, boost::lockfree::capacity<1024> > retire_queue;
};
}

#endif
