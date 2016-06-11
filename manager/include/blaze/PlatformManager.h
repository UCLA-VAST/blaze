#ifndef PLATFORM_MANAGER_H
#define PLATFORM_MANAGER_H

#include <string>
#include <vector>
#include <map>

#include <boost/smart_ptr.hpp>

#include "proto/acc_conf.pb.h"
#include "Common.h"

namespace blaze {

class PlatformManager 
: public boost::basic_lockable_adapter<boost::mutex>
{

  friend class AppCommManager;

public:
  
  PlatformManager(ManagerConf *conf);

  bool accExists(std::string acc_id);
  bool platformExists(std::string platform);

  Platform* getPlatformByAccId(std::string acc_id);

  Platform* getPlatformById(std::string platform_id);

  TaskManager_ref getTaskManager(std::string acc_id);

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

  void removeAcc(
      std::string requester,
      std::string acc_id,
      std::string platform_id);

  // map platform_id to Platform 
  std::map<std::string, Platform_ptr> platform_table;

  // map acc_id to accelerator platform
  std::map<std::string, std::string> acc_table;

  // map acc_id to BlockManager platform
  // TODO: should be deprecated
  std::map<std::string, std::string> cache_table;
};
} // namespace blaze
#endif
