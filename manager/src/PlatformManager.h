#ifndef PLATFORM_MANAGER_H
#define PLATFORM_MANAGER_H

#include <string>
#include <vector>
#include <map>

#include <boost/smart_ptr.hpp>

#include "proto/acc_conf.pb.h"
#include "Common.h"

namespace blaze {

class PlatformManager {
  friend class AppCommManager;

public:
  
  PlatformManager(ManagerConf *conf);

  Platform* getPlatformByAccId(std::string acc_id);

  Platform* getPlatformById(std::string platform_id);

  TaskManager* getTaskManager(std::string acc_id);

  AccWorker getConfig(std::string acc_id) {
    // exception should be handled by previous steps
    return acc_config_table[acc_id];
  }

  // remove a shared block from all platforms
  void removeShared(int64_t block_id);

  std::vector<std::pair<std::string, std::string> > getLabels();

private:
  // create a new platform from file
  Platform_ptr create(
      std::string id, 
      std::map<std::string, std::string> &conf_table);

  void registerAcc(
      std::string platform_id, 
      AccWorker &acc_conf);

  // map platform_id to Platform 
  std::map<std::string, Platform_ptr> platform_table;

  // TODO
  // map acc_id to TaskManager
  //std::map<std::string, TaskManager_ptr> task_manager_table;

  // map acc_id to accelerator platform
  std::map<std::string, std::string> acc_table;

  // map acc_id to BlockManager platform
  std::map<std::string, std::string> cache_table;

  // map acc_id to AccWorker (acc configuration)
  std::map<std::string, AccWorker> acc_config_table;
};
} // namespace blaze
#endif
