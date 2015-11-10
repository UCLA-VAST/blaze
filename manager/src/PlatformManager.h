#ifndef PLATFORM_MANAGER_H
#define PLATFORM_MANAGER_H

#include <string>
#include <vector>
#include <map>

#include <boost/smart_ptr.hpp>

#include "proto/acc_conf.pb.h"

#include "BlockManager.h"
#include "QueueManager.h"
#include "TaskManager.h"
#include "Platform.h"
#include "Logger.h"
#include "Block.h"

namespace blaze {

class PlatformManager {

public:
  
  PlatformManager(ManagerConf *conf,
      Logger *_logger);

  BlockManager* getBlockManager(std::string acc_id) {
    if (acc_table.find(acc_id) != acc_table.end()) {
      return block_manager_table[acc_table[acc_id]].get();
    }
    else {
      return NULL;
    }
  }

  TaskManager_ptr getTaskManager(std::string acc_id) {
    if (acc_table.find(acc_id) == acc_table.end() || 
        queue_manager_table.find(acc_table[acc_id]) == 
          queue_manager_table.end())
    {
      return NULL_TASK_MANAGER;
    }
    else {
      return queue_manager_table[acc_table[acc_id]]->get(acc_id);  
    }
  }

  AccWorker getConfig(std::string acc_id) {
    // exception should be handled by previous steps
    return acc_config_table[acc_id];
  }


  DataBlock_ptr getShared(int64_t block_id);
  void addShared(int64_t block_id, DataBlock_ptr block);
  void removeShared(int64_t block_id);

  Platform_ptr create(std::string id);

  std::vector<std::string> getAccNames();

private:
  Logger *logger;

  // map platform_id to Platform 
  std::map<std::string, Platform_ptr> platform_table;

  // map acc_id to platform_id
  std::map<std::string, std::string> acc_table;

  // map acc_id to AccWorker (acc configuration)
  std::map<std::string, AccWorker> acc_config_table;

  // map platform_id to BlockManager
  std::map<std::string, BlockManager_ptr> block_manager_table;

  // map platform_id to QueueManager
  std::map<std::string, QueueManager_ptr> queue_manager_table;
};
} // namespace blaze
#endif
