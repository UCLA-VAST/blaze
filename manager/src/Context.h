#ifndef CONTEXT_H
#define CONTEXT_H

#include <string>
#include <vector>
#include <map>

#include <boost/smart_ptr.hpp>

#include "acc_conf.pb.h"

#include "BlockManager.h"
#include "QueueManager.h"
#include "TaskManager.h"
#include "Logger.h"
#include "TaskEnv.h"
#include "OpenCLEnv.h"
#include "Block.h"
#include "OpenCLBlock.h"

namespace acc_runtime {

class Context {

public:
  Context(Logger* _logger): logger(_logger) {;}
  
  Context(ManagerConf *conf,
      Logger *_logger,
      QueueManager *_manager);

  TaskEnv* getEnv(std::string acc_id) {
     if (acc_table.find(acc_id) != acc_table.end()) {
      return acc_table[acc_id].get();  
    }
    else {
      return NULL;
    }
  }

  TaskEnv* getEnv(AccType type) {
    if (env_table.find(type) != env_table.end()) {
      return env_table[type].get();  
    }
    else {
      return NULL;
    }
  }

  BlockManager* getBlockManager(std::string acc_id) {
    if (acc_table.find(acc_id) != acc_table.end()) {
      return block_manager_table[acc_table[acc_id]->getType()].get();
    }
    else {
      return NULL;
    }
  }

  void addShared(int64_t block_id, DataBlock_ptr block);

  DataBlock_ptr getShared(int64_t block_id);

  void removeShared(int64_t block_id);

private:
  QueueManager *queue_manager;
  Logger *logger;

  // map AccType to TaskEnv
  std::map<AccType, TaskEnv_ptr> env_table;

  // map acc_id to TaskEnv
  std::map<std::string, TaskEnv_ptr> acc_table;

  // map AccType to BlockManager
  std::map<AccType, BlockManager_ptr> block_manager_table;
};
}
#endif 
