#ifndef ACCAGENT_H
#define ACCAGENT_H

#include <boost/atomic.hpp>

#include "Common.h"
#include "Task.h"
#include "proto/acc_conf.pb.h"

namespace blaze {

typedef boost::shared_ptr<ManagerConf> ManagerConf_ptr;

class AccAgent {
 public:
  AccAgent(const char* conf_path);

  Task_ptr createTask(std::string acc_id);
  
  void writeInput(Task_ptr task,
                  std::string acc_id,
                  void* data_ptr,
                  int num_items, 
                  int item_length, 
                  int data_width);

  void readOutput(Task_ptr task,
                  void*    data_ptr,
                  size_t   data_size);

 private:
  ManagerConf_ptr     conf_;
  PlatformManager_ptr platform_manager_;
};

} // namespace blaze
#endif
