#ifndef PLATFORM_H
#define PLATFORM_H

#include <stdio.h>
#include <boost/smart_ptr.hpp>
#include <string>
#include <map>

#include "proto/acc_conf.pb.h"
#include "Common.h"
#include "TaskEnv.h"
#include "QueueManager.h"

namespace blaze {

class Platform {
  
public:
  // setup platform context in the constructor, exceptions can be thrown
  // must initialize env if there is no exception
  Platform() {
    env = new TaskEnv();
  }

  ~Platform() {
    delete env;  
  }

  // store an accelerator setup on the platform
  void setupAcc(AccWorker &conf) {
    if (acc_table.find(conf.id()) == acc_table.end()) {
      acc_table.insert(std::make_pair(conf.id(), conf));
    }
  }

  // create a platform-specific queue manager
  virtual QueueManager_ptr createQueue() {
    QueueManager_ptr queue(new QueueManager(this));
    return queue;
  }

  // create a block object for the specific platform
  virtual DataBlock_ptr createBlock(
      int num_items, 
      int item_length,
      int item_size, 
      int align_width = 0) 
  {
    return env->createBlock(num_items, item_length, item_size, align_width);
  }

  // get an entry in the config_table matching the key
  std::string getConfig(std::string &key) {
    if (config_table.find(key)==config_table.end()) {
      return std::string();
    } else {
      return config_table[key];
    }
  }

  // get TaskEnv to pass to Task
  TaskEnv* getEnv() {return env;}

protected:
  // a table storing platform configurations mapped by key
  std::map<std::string, std::string> config_table;

  // a table storing platform configurations mapped by key
  std::map<std::string, AccWorker> acc_table;

private:
  TaskEnv* env;
};

} // namespace blaze
#endif
