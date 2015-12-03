#ifndef PLATFORM_H
#define PLATFORM_H

#include <stdio.h>
#include <boost/smart_ptr.hpp>
#include <string>
#include <map>

#include "proto/acc_conf.pb.h"
#include "Common.h"

namespace blaze {

class Platform {
  
public:
  Platform();

  // store an accelerator setup on the platform
  void setupAcc(AccWorker &conf);

  // create a platform-specific queue manager
  virtual QueueManager_ptr createQueue(); 

  // create a block object for the specific platform
  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0);

  // get an entry in the config_table matching the key
  std::string getConfig(std::string &key);

  // get TaskEnv to pass to Task
  virtual TaskEnv_ptr getEnv(std::string id);

protected:
  // a table storing platform configurations mapped by key
  std::map<std::string, std::string> config_table;

  // a table storing platform configurations mapped by key
  std::map<std::string, AccWorker> acc_table;

private:
  TaskEnv_ptr env;
};

} // namespace blaze
#endif
