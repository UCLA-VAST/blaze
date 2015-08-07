
#include "Context.h"

#define LOG_HEADER  std::string("Context::") + \
                    std::string(__func__) +\
                    std::string("(): ")

namespace acc_runtime {

Context::Context(
    ManagerConf *conf,
    Logger *_logger,
    QueueManager *_manager):
  logger(_logger), queue_manager(_manager)
{

  for (int i=0; i<conf->platform_size(); i++) {

    AccPlatform platform = conf->platform(i);
    AccType type = platform.type();
    TaskEnv_ptr task_env;

    switch (type) {
      case AccType::CPU :
        try {
          TaskEnv_ptr env(new TaskEnv(AccType::CPU));

          env_table.insert(std::make_pair(type, env));

          int cache_limit = platform.cache_limit();
          int scratch_limit = platform.scratch_limit();

          BlockManager_ptr block_manager(
              new BlockManager(
                env.get(), 
                _logger,
                (size_t)cache_limit << 20,
                (size_t)scratch_limit << 20)
              );

          block_manager_table.insert(
              std::make_pair(type, block_manager));

          task_env = env;
        } 
        catch (std::runtime_error &e) {
          logger->logErr(
              LOG_HEADER +
              std::string("cannot create OpenCL env"));
        }
        break;
      case AccType::OpenCL :
        try {

          // create task environment
          TaskEnv_ptr env(new OpenCLEnv(AccType::OpenCL));

          env_table.insert(std::make_pair(type, env));

          // get block manager settings
          AccType cache_loc     = platform.cache_loc();
          int     cache_limit   = platform.cache_limit();
          int     scratch_limit = platform.scratch_limit();

          // create block manager
          if (block_manager_table.find(cache_loc) != 
              block_manager_table.end()) 
          {
            // if the cache is shared with another platform
            block_manager_table.insert(
                std::make_pair(type, block_manager_table[cache_loc]));
          }
          else 
          {
            BlockManager_ptr block_manager(
                new BlockManager(
                  env.get(), 
                  _logger,
                  (size_t)cache_limit << 20,
                  (size_t)scratch_limit << 20)
                );

            block_manager_table.insert(
                std::make_pair(type, block_manager));
          }
          task_env = env;

          logger->logInfo(
              LOG_HEADER +
              std::string("created OpenCLEnv()"));

        } catch (std::runtime_error &e) {
          logger->logErr(
              LOG_HEADER +
              std::string("cannot create OpenCL env"));
        }
        break;
      default :
        break;
    }  
    // add accelerators to the platform
    for (int j=0; j<platform.acc_size(); j++) {

      try {
        AccWorker acc = platform.acc(j);

        // add acc mapping to table
        acc_table.insert(std::make_pair(
              acc.id(), task_env));

        // setup the task environment with ACC conf
        task_env->setup(acc);

        // create a corresponding task manager 
        queue_manager->add(acc.id(), acc.path(), task_env.get());
      } 
      catch (std::runtime_error &e) {
        logger->logErr(
            LOG_HEADER +
            std::string("cannot create acc ")+
            platform.acc(j).id());
      }
    }
  }
}

void Context::addShared(
    int64_t block_id, 
    DataBlock_ptr block) 
{
  try {
    for (std::map<AccType, BlockManager_ptr>::iterator 
        iter = block_manager_table.begin(); 
        iter != block_manager_table.end(); 
        iter ++) 
    {
      switch (iter->first) {
        case AccType::CPU :
        {
          iter->second->add(block_id, block);
          break;
        }
        case AccType::OpenCL :
        {
          // copy the block to OpenCL env
          DataBlock_ptr new_block(
              new OpenCLBlock(
                dynamic_cast<OpenCLEnv*>(env_table[AccType::OpenCL].get()),
                block.get()));

          iter->second->add(block_id, new_block);
          break;
        }
        default: ;
      }
    }
  }
  catch (std::runtime_error &e) {
    throw(e);
  }
}

void Context::removeShared(int64_t block_id)
{
  try {
    for (std::map<AccType, BlockManager_ptr>::iterator 
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

DataBlock_ptr Context::getShared(int64_t block_id)
{
  try {
    DataBlock_ptr cpu_block;

    for (std::map<AccType, BlockManager_ptr>::iterator 
        iter = block_manager_table.begin(); 
        iter != block_manager_table.end(); 
        iter ++) 
    {
      DataBlock_ptr block = iter->second->get(block_id);
      if (block == NULL_DATA_BLOCK) {
        return NULL_DATA_BLOCK;
      }
      if (iter->first == AccType::CPU) {
        cpu_block = block;
      }
    }
    return cpu_block;
  }
  catch (std::runtime_error &e) {
    throw(e);
  }
}

} // namespace acc_runtime

