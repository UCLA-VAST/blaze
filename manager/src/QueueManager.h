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

#include "acc_conf.pb.h"

#include "TaskEnv.h"
#include "Task.h"
#include "TaskManager.h"
#include "Logger.h"

namespace blaze {

typedef boost::shared_ptr<TaskManager> TaskManager_ptr;
const TaskManager_ptr NULL_TASK_MANAGER;

class QueueManager {

public:
  QueueManager(Logger *_logger): logger(_logger) {;}

  // build task queues for all libraries in a path
  //void buildFromPath(std::string lib_dir);

  // build task queues from a configuration file
  //void buildFromConf(ManagerConf *conf);

  // add a new queue regarding an existing accelerator
  void add(
    std::string id, 
    std::string lib_path, 
    TaskEnv *env);

  // request the task manager by acc id
  TaskManager_ptr get(std::string id);

  // start the executor and commiter for one task queue
  void start(std::string id);

  // start the executor and commiter for all queues
  void startAll();

private:
  std::map<std::string, TaskManager_ptr> queue_table;
  Logger *logger;
};
}

#endif
