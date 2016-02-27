#ifndef QUEUE_MANAGER_H
#define QUEUE_MANAGER_H

#include "proto/acc_conf.pb.h"
#include "Common.h"

namespace blaze {

class QueueManager 
: public boost::basic_lockable_adapter<boost::mutex>
{

public:
  QueueManager(Platform *_platform): 
    platform(_platform)
  {;}

  // add a new queue regarding an existing accelerator
  virtual void add(std::string id, std::string lib_path);

  // request the task manager by acc id
  TaskManager_ptr get(std::string id);

  // remove a task manager in queue_table
  void remove(std::string id);

  // start the executor for one task queue
  virtual void start(std::string id);

  // read TaskEnv for scheduling
  TaskEnv* getTaskEnv(Task* task);

protected:
  void setTaskEnv(Task* task, TaskEnv_ptr env);

  DataBlock_ptr getTaskInputBlock(Task* task, int idx);
  void setTaskInputBlock(Task* task, DataBlock_ptr block, int idx);

  std::map<std::string, TaskManager_ptr> queue_table;
  std::map<std::string, void*>           tasklib_table;

  Platform *platform;
};
}

#endif
