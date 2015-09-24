#ifndef PLATFORM_H
#define PLATFORM_H

#include <stdio.h>
#include <boost/smart_ptr.hpp>
#include <string>
#include <map>

#include "TaskEnv.h"
#include "Block.h"

namespace blaze {

class AccWorker;

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

  // setup an accelerator on the platform
  virtual void setupAcc(AccWorker &conf) {;}

  // create a block object for the specific platform
  virtual DataBlock_ptr createBlock() {
    DataBlock_ptr block(new DataBlock());
    return block;
  }

  virtual DataBlock_ptr createBlock(size_t length, size_t size) {
    DataBlock_ptr block(new DataBlock(length, size));
    return block;
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
  TaskEnv* env;
  
  // a table storing platform configurations mapped by key
  std::map<std::string, std::string> config_table;
};

typedef boost::shared_ptr<Platform> Platform_ptr;
} // namespace blaze
#endif
