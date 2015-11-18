#include <fstream>
#include <stdexcept>
#include <dlfcn.h>

#include <glog/logging.h>
#include "PlatformManager.h"

namespace blaze {

PlatformManager::PlatformManager(ManagerConf *conf)
{
  for (int i=0; i<conf->platform_size(); i++) {

    AccPlatform platform_conf = conf->platform(i);

    std::string id   = platform_conf.id();
    std::string path = platform_conf.path();

    try {
      Platform_ptr platform = this->create(path);

      if (platform_table.find(id) != platform_table.end()) {
        throw std::runtime_error("Duplicated platform found: " + id);
      }
      platform_table.insert(std::make_pair(id, platform));

      int        cache_limit   = platform_conf.cache_limit();
      int        scratch_limit = platform_conf.scratch_limit();
      std::string cache_loc    = platform_conf.cache_loc();

      // create block manager
      if (block_manager_table.find(cache_loc) != 
          block_manager_table.end()) 
      {
        // if the cache is shared with another platform
        block_manager_table.insert(
            std::make_pair(id, block_manager_table[cache_loc]));
      }
      else 
      {
        BlockManager_ptr block_manager(
            new BlockManager(
              platform.get(),
              (size_t)cache_limit << 20,
              (size_t)scratch_limit << 20)
            );

        block_manager_table.insert(
            std::make_pair(id, block_manager));
      }

      // create queue manager
      QueueManager_ptr queue_manager = platform->createQueue();

      // add the new queue manager to queue table
      queue_manager_table.insert(std::make_pair(id, queue_manager));

      // add accelerators to the platform
      for (int j=0; j<platform_conf.acc_size(); j++) {

        AccWorker acc_conf = platform_conf.acc(j);
        try {

          // check if acc of the same already exists
          if (acc_table.find(acc_conf.id()) != acc_table.end()) {
            throw std::runtime_error(
                "accelerator of the same id already exists");
          }

          // add acc mapping to table
          acc_table.insert(std::make_pair(
                acc_conf.id(), id));

          // add acc configuration to table
          acc_config_table.insert(
              std::make_pair(acc_conf.id(), acc_conf));

          // setup the task environment with ACC conf
          platform->setupAcc(acc_conf);

          // create a corresponding task manager 
          queue_manager->add(acc_conf.id(), acc_conf.path());
        } 
        catch (std::exception &e) {
          LOG(ERROR) << "Cannot create ACC " << 
              acc_conf.id() <<
              ": " << e.what();
        }
      }

      // start all executors/commiters in QueueManager
      queue_manager->startAll();
    }
    catch (std::runtime_error &e) {
      LOG(ERROR) << "Cannot create platform " << id <<
        ": " << e.what();
    }
  }
}

// create a new platform
Platform_ptr PlatformManager::create(std::string path) {

  if (path.compare("")==0) {
    Platform_ptr platform(new Platform());
    return platform;
  }
  else {
    void* handle = dlopen(path.c_str(), RTLD_LAZY|RTLD_GLOBAL);

    if (handle == NULL) {
      throw std::runtime_error(dlerror());
    }

    // reset errors
    dlerror();

    // load the symbols
    Platform* (*create_func)();
    void (*destroy_func)(Platform*);

    // read the custom constructor and destructor  
    create_func = (Platform* (*)())dlsym(handle, "create");
    destroy_func = (void (*)(Platform*))dlsym(handle, "destroy");

    const char* error = dlerror();
    if (error) {
      throw std::runtime_error(error);
    }

    // TODO: exception handling?
    Platform_ptr platform(create_func(), destroy_func);

    return platform;
  }
}

void PlatformManager::removeShared(int64_t block_id)
{
  try {
    for (std::map<std::string, BlockManager_ptr>::iterator 
        iter = block_manager_table.begin(); 
        iter != block_manager_table.end(); 
        iter ++) 
    {
      iter->second->remove(block_id);
    }
  }
  catch (std::runtime_error &e) {
    throw(e);
  }
}

int PlatformManager::getNumAcc(std::string platform_id) {

  if (queue_manager_table.find(platform_id) == queue_manager_table.end()) 
  {
    return 0; 
  }
  else { 
    return queue_manager_table[platform_id]->getNumAcc();
  }
}

int PlatformManager::getNumAcc() {
  int count = 0; 
  std::map<std::string, QueueManager_ptr>::iterator iter;

  for (iter =  queue_manager_table.begin();
       iter != queue_manager_table.end();
       iter ++)
  {
    count += iter->second->getNumAcc(); 
  }
  return count;
}

std::vector<std::string> PlatformManager::getAccNames() {
  std::vector<std::string> ret;
  std::map<std::string, std::string>::iterator iter;
  for (iter = acc_table.begin();
       iter != acc_table.end();
       iter ++ )
  {
    ret.push_back(iter->first); 
  }
  return ret;
}
} // namespace blaze

