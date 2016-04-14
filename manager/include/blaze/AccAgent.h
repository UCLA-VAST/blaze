#ifndef ACCAGENT_H
#define ACCAGENT_H

#include "Common.h"
#include "proto/acc_conf.pb.h"

namespace blaze {

typedef boost::shared_ptr<ManagerConf> ManagerConf_ptr;

class AccAgent {
 public:
  AccAgent(const char* conf_path);

  int getPort() {return port_;}

 private:
  int port_;
  ManagerConf_ptr     conf_;
  CommManager_ptr     comm_manager_;
  PlatformManager_ptr platform_manager_;
};

} // namespace blaze
#endif
