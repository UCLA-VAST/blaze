#ifndef TASK_MANAGER_H
#define TASK_MANAGER_H

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/atomic.hpp>

#include "Common.h"
#include "TaskQueue.h"

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
    std::string _acc_id,
    Platform *_platform
  ): power(true),
     scheduler_idle(true),
     executor_idle(true),
     nextTaskId(0),
     acc_id(_acc_id),
     createTask(create_func),
     destroyTask(destroy_func),
     platform(_platform)
  {;}

  ~TaskManager() {
    power = false; 
    task_workers.join_all();
  }

  // create a task and return the task pointer
  Task_ptr create();

  // enqueue a task in the corresponding application queue
  void enqueue(std::string app_id, Task* task);

  // dequeue a task from the execute queue
  bool popReady(Task* &task);

  // get best and worst cast wait time 
  std::pair<int, int> getWaitTime(Task* task);

  void startExecutor();
  void startScheduler();

  void start();
  void stop();
  bool isBusy();

  bool isEmpty();

  // experimental
  std::string getConfig(int idx, std::string key);

private:

  boost::thread_group task_workers;

  // schedule a task from app queues to execution queue  
  bool schedule();

  // execute front task in the queue
  bool execute();

  // Enable signal all the worker threads (scheduler, executor)
  bool power;

  // These two flag let TaskManager exits gracefully:
  // When power=false, but the app_queues and execution_queue
  // are still not empty, clear the queue before exits
  bool scheduler_idle;
  bool executor_idle;

  // Locate TaskEnv and for logging purpose
  std::string acc_id;

  mutable boost::atomic<int> nextTaskId;

  // Task implementation loaded from user acc_impl
  Task* (*createTask)();
  void (*destroyTask)(Task*);

  Platform *platform;

  // thread function body for scheduler and executor
  void do_schedule();
  void do_execute();

  void updateDelayModel(Task* task, int estimateTime, int realTime);

  // application queues mapped by application id
  std::map<std::string, TaskQueue_ptr> app_queues;

  TaskQueue execution_queue;
};

const TaskManager_ptr NULL_TASK_MANAGER;
}

#endif
