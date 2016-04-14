#ifndef ADMIN_H
#define ADMIN_H

#include "proto/task.pb.h"
#include "proto/acc_conf.pb.h"

#include "Common.h"

// for testing purpose
#ifndef TEST_FRIENDS_LIST
#define TEST_FRIENDS_LIST
#endif

namespace blaze {

class Admin {
  TEST_FRIENDS_LIST
public:
  Admin(std::string _ip = "127.0.0.1");

  bool registerAcc(ManagerConf &conf);
  bool deleteAcc(ManagerConf &conf);

private:

  void sendMessage(TaskMsg &msg);

  // data structures for socket connection
  int nam_port;
  std::string ip_address;
  ios_ptr ios;
  endpoint_ptr endpoint;
};

} // namespace blaze
#endif
