#include <fstream>
#include <stdexcept>
#include <dlfcn.h>

#include "PlatformManager.h"

#define LOG_HEADER  std::string("PlatformManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")

namespace blaze {

PlatformManager::PlatformManager(
    ManagerConf *conf,
    Logger *_logger): logger(_logger)
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
              _logger,
              (size_t)cache_limit << 20,
              (size_t)scratch_limit << 20)
            );

        block_manager_table.insert(
            std::make_pair(id, block_manager));
      }

      // TODO: use different queue manager for each platform
      // create queue manager
      QueueManager_ptr queue_manager(new QueueManager(platform.get(), logger));

      // add the new queue manager to queue table
      queue_manager_table.insert(std::make_pair(id, queue_manager));

      // add accelerators to the platform
      for (int j=0; j<platform_conf.acc_size(); j++) {

        try {
          AccWorker acc_conf = platform_conf.acc(j);

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
          logger->logErr(
              LOG_HEADER +
              std::string("cannot create acc ")+
              platform_conf.acc(j).id() +
              std::string(": ") + e.what());
        }
      }

      // start all executors/commiters in QueueManager
      queue_manager->startAll();
    }
    catch (std::runtime_error &e) {
      logger->logErr(LOG_HEADER +
        std::string("Cannot create platform ") + id +
        std::string(": ") + e.what());
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
      logger->logErr(LOG_HEADER + dlerror());
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
      logger->logErr(LOG_HEADER + error);
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
} // namespace blaze

