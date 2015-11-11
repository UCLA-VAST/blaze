#ifndef QUEUE_MANAGER_H
#define QUEUE_MANAGER_H

#include <map>
#include <vector>
#include <iostream>

#include <boost/smart_ptr.hpp>
#include <boost/filesystem.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include "proto/acc_conf.pb.h"

#include "Platform.h"
#include "Task.h"
#include "TaskManager.h"
#include "Logger.h"

namespace blaze {

typedef boost::shared_ptr<TaskManager> TaskManager_ptr;
const TaskManager_ptr NULL_TASK_MANAGER;

class QueueManager {

public:
  QueueManager(Platform *_platform): 
    platform(_platform)
  {;}

  // add a new queue regarding an existing accelerator
  void add(
    std::string id, 
    std::string lib_path);

  // request the task manager by acc id
  TaskManager_ptr get(std::string id);

  // start the executor and commiter for one task queue
  virtual void start(std::string id);

  // start the executor and commiter for all queues
  virtual void startAll();

protected:
  std::map<std::string, TaskManager_ptr> queue_table;
  Platform *platform;
};

typedef boost::shared_ptr<QueueManager> QueueManager_ptr;
}

#endif
