#ifndef QUEUE_MANAGER_H
#define QUEUE_MANAGER_H

#include <map>
#include <vector>
#include <iostream>

#include "proto/acc_conf.pb.h"
#include "Common.h"

namespace blaze {

class Platform;
class TaskManager;

class QueueManager {

public:
  QueueManager(Platform *_platform): 
    platform(_platform)
  {;}

  // add a new queue regarding an existing accelerator
  virtual void add(
    std::string id, 
    std::string lib_path);

  // request the task manager by acc id
  TaskManager_ptr get(std::string id);

  // start the executor and commiter for one task queue
  virtual void start(std::string id);

  // start the executor and commiter for all queues
  virtual void startAll();

  // read TaskEnv for scheduling
  TaskEnv* getTaskEnv(Task* task);

protected:
  void setTaskEnv(Task* task, TaskEnv_ptr env);

  DataBlock_ptr getTaskInputBlock(Task* task, int idx);

  std::map<std::string, TaskManager_ptr> queue_table;

  Platform *platform;
};
}

#endif
