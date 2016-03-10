#include <fstream>
#include <stdexcept>
#include <dlfcn.h>

#include <boost/regex.hpp>

#define LOG_HEADER "PlatformManager"
#include <glog/logging.h>

#include "BlockManager.h"
#include "Platform.h"
#include "PlatformManager.h"
#include "QueueManager.h"
#include "TaskManager.h"

namespace blaze {

PlatformManager::PlatformManager(ManagerConf *conf)
{
  for (int i=0; i<conf->platform_size(); i++) {

    AccPlatform platform_conf = conf->platform(i);

    std::string id   = platform_conf.id();
    std::string path = platform_conf.path();

    // check platform id for special characters
    boost::regex special_char_test("\\w+", boost::regex::perl);
    if (!boost::regex_match(id.begin(), id.end(), special_char_test)) {
      LOG(ERROR) << "Platform id [" << id << "] cannot contain " 
        << "special characters beside alphanumeric and '_'";
      continue;
    }

    try {
      // generic Platform configuration
      if (platform_table.find(id) != platform_table.end()) {
        throw std::runtime_error("Duplicated platform found: " + id);
      }

      int cache_limit   = platform_conf.cache_limit();
      int scratch_limit = platform_conf.scratch_limit();

      std::string cache_loc = platform_conf.has_cache_loc() ? 
                                platform_conf.cache_loc() : id;

      // extended Platform configurations
      std::map<std::string, std::string> conf_table;
      for (int i=0; i<platform_conf.param_size(); i++) {
        std::string conf_key   = platform_conf.param(i).key();
        std::string conf_value = platform_conf.param(i).value();
        conf_table[conf_key]   = conf_value;
      }

      // create Platform
      Platform_ptr platform = this->create(path, conf_table);

      platform_table.insert(std::make_pair(id, platform));

      // create block manager
      if (cache_table.find(cache_loc) == cache_table.end())
      {
        if (cache_loc.compare(id) != 0) {
          LOG(WARNING) << "Unspecificed cache location, use private instead";
          cache_loc = id;
        }
        // if the cache is not shared with another platform
        // create a block manager in current platform
        platform->createBlockManager(
            (size_t)cache_limit << 20, 
            (size_t)scratch_limit << 20);

        DLOG(INFO) << "Create a block manager for " << cache_loc;
      }
      cache_table.insert(std::make_pair(id, cache_loc));
      DLOG(INFO) << "Config platform " << id << 
        " to use device memory on " << cache_loc;

      // print extend configs
      if (!conf_table.empty()) {
        VLOG(1) << "Extra Configurations for the platform:";
        std::map<std::string, std::string>::iterator iter;
        for (iter  = conf_table.begin();
            iter != conf_table.end();
            iter ++)
        {
          VLOG(1) << "[""" << iter->first << """] = "
            << iter->second;
        }
      }
      cache_table.insert(std::make_pair(id, cache_loc));

      // add accelerators to the platform
      for (int j=0; j<platform_conf.acc_size(); j++) {

        AccWorker acc_conf = platform_conf.acc(j);
        try {
          registerAcc(id, acc_conf);          
        } 
        catch (std::exception &e) {
          LOG(ERROR) << "Cannot create ACC " << 
              acc_conf.id() <<
              ": " << e.what();
        }
      }
    }
    catch (std::runtime_error &e) {
      LOG(ERROR) << "Cannot create platform " << id <<
        ": " << e.what();
    }
  }
}

bool PlatformManager::accExists(std::string acc_id) {
  boost::lock_guard<PlatformManager> guard(*this);
  return acc_table.find(acc_id) != acc_table.end();
}

bool PlatformManager::platformExists(std::string platform_id) {
  // boost::lock_guard<PlatformManager> guard(*this);
  return platform_table.find(platform_id) != platform_table.end();
}

Platform* PlatformManager::getPlatformByAccId(std::string acc_id) {

  boost::lock_guard<PlatformManager> guard(*this);
  if (acc_table.find(acc_id) == acc_table.end()) {
    return NULL;
  } else {
    return platform_table[acc_table[acc_id]].get();
  }
}

Platform* PlatformManager::getPlatformById(std::string platform_id) {

  boost::lock_guard<PlatformManager> guard(*this);
  if (platform_table.find(platform_id) == platform_table.end()) {
    return NULL;
  } else {
    return platform_table[platform_id].get();
  }
}

TaskManager_ref PlatformManager::getTaskManager(std::string acc_id) {
  // lock all tables to guarantee exclusive access
  boost::lock_guard<PlatformManager> guard(*this);
  if (acc_table.find(acc_id) == acc_table.end()) {
    TaskManager_ref ret;
    return ret;
  } else {
    return platform_table[acc_table[acc_id]]->getTaskManager(acc_id);  
  }
}

void PlatformManager::registerAcc(
    std::string platform_id, 
    AccWorker &acc_conf) 
{
  // lock all tables to guarantee exclusive access
  boost::lock_guard<PlatformManager> guard(*this);

  DLOG(INFO) << "Adding acc: " << acc_conf.id();

  // check if acc of the same already exists
  if (acc_table.find(acc_conf.id()) != acc_table.end()) {
    throw commError(
        "Accelerator already exists");
  }
  Platform_ptr platform = platform_table[platform_id];

  if (!platform) {
    throw commError(
        "Required platform does not exist");
  }

  // setup the task environment with ACC conf
  platform->addQueue(acc_conf);

  // add acc mapping to table
  acc_table.insert(std::make_pair(
        acc_conf.id(), platform_id));

  // add cache mapping to table
  cache_table.insert(std::make_pair(
        acc_conf.id(), platform_id));

  VLOG(1) << "Added an accelerator queue "
          << "[" << acc_conf.id() << "] "
          << "for platform: " << platform_id;
}

void PlatformManager::removeAcc(
    std::string requester,  // TODO: used to verify client ID
    std::string acc_id,
    std::string platform_id) 
{
  // lock all tables to guarantee exclusive access
  boost::lock_guard<PlatformManager> guard(*this);

  // check if acc of the same already exists
  if (acc_table.find(acc_id) == acc_table.end()) {
    return;
  }
  Platform_ptr platform = platform_table[platform_id];

  if (!platform) {
    throw std::runtime_error(
        "required platform does not exist");
  }

  // remove mappings of acc_id
  acc_table.erase(acc_id);
  cache_table.erase(acc_id);

  // setup the task environment with ACC conf
  platform->removeQueue(acc_id);

  VLOG(1) << "Removed an accelerator queue "
          << "[" << acc_id << "] "
          << "for platform: " << platform_id;
}

// create a new platform
Platform_ptr PlatformManager::create(
    std::string path, 
    std::map<std::string, std::string> &conf_table) 
{
  if (path.compare("")==0) {
    Platform_ptr platform(new Platform(conf_table));
    return platform;
  }
  else {
    void* handle = dlopen(path.c_str(), RTLD_LAZY|RTLD_LOCAL);

    if (handle == NULL) {
      throw std::runtime_error(dlerror());
    }

    // reset errors
    dlerror();

    // load the symbols
    Platform* (*create_func)(std::map<std::string, std::string>&);
    void (*destroy_func)(Platform*);

    // read the custom constructor and destructor  
    create_func = (Platform* (*)(std::map<std::string, std::string>&))
                    dlsym(handle, "create");
    destroy_func = (void (*)(Platform*))dlsym(handle, "destroy");

    const char* error = dlerror();
    if (error) {
      throw std::runtime_error(error);
    }

    Platform_ptr platform(create_func(conf_table), destroy_func);

    return platform;
  }
}

void PlatformManager::removeShared(int64_t block_id)
{
  try {
    for (std::map<std::string, std::string>::iterator 
        iter = cache_table.begin(); 
        iter != cache_table.end(); 
        iter ++) 
    {
      platform_table[iter->second]->remove(block_id);
    }
  }
  catch (std::runtime_error &e) {
    throw(e);
  }
}

std::vector<std::pair<std::string, std::string> > PlatformManager::getLabels()
{
  std::vector<std::pair<std::string, std::string> > ret;
  std::map<std::string, std::string>::iterator iter;
  for (iter = acc_table.begin();
       iter != acc_table.end();
       iter ++ )
  {
    ret.push_back(std::make_pair(iter->first, iter->second)); 
  }
  return ret;
}

} // namespace blaze

