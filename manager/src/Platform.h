#ifndef PLATFORM_H
#define PLATFORM_H

#include "proto/acc_conf.pb.h"
#include "Common.h"

namespace blaze {

class Platform {
  friend class PlatformManager;  

public:
  Platform(std::map<std::string, std::string> &conf_table);

  virtual void addQueue(AccWorker &conf);
  virtual void removeQueue(std::string id);

  // store an accelerator setup on the platform
  //virtual void setupAcc(AccWorker &conf);

  // obtain a BlockManager
  virtual void createBlockManager(size_t cache_limit, size_t scratch_limit);

  virtual BlockManager* getBlockManager();

  virtual TaskManager_ref getTaskManager(std::string id);

  virtual QueueManager* getQueueManager();

  // create a block object for the specific platform
  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0, 
      int flag = BLAZE_INPUT_BLOCK);

  virtual DataBlock_ptr createBlock(const DataBlock&);

  virtual void remove(int64_t block_id);

  // get an entry in the config_table matching the key
  std::string getConfig(std::string &key);

  // get TaskEnv to pass to Task
  virtual TaskEnv_ptr getEnv(std::string id);

protected:
  BlockManager_ptr block_manager;
  QueueManager_ptr queue_manager;

  // a table storing platform configurations mapped by key
  std::map<std::string, std::string> config_table;

  // a table storing platform configurations mapped by key
  std::map<std::string, AccWorker> acc_table;

private:
  TaskEnv_ptr env;
};

} // namespace blaze
#endif
