#ifndef COMM_H
#define COMM_H

#include <google/protobuf/message.h>

#include "proto/task.pb.h"
#include "Common.h"

// for testing purpose
#ifndef TEST_FRIENDS_LIST
#define TEST_FRIENDS_LIST
#endif

using namespace boost::asio;

namespace blaze {

/*
 * Communicator design for Node Manager
 */
class CommManager
: public boost::basic_lockable_adapter<boost::mutex>
{
  TEST_FRIENDS_LIST
public:
  CommManager(
      PlatformManager* _platform,
      std::string address = "127.0.0.1",
      int ip_port = 1027,
      int _max_threads = boost::thread::hardware_concurrency());

  ~CommManager();

protected:
  // pure virtual method called by listen
  virtual void process(socket_ptr) = 0;

  // reference to platform manager
  PlatformManager *platform_manager;

private:
  void startAccept();
  void handleAccept(
      const boost::system::error_code& error,
      socket_ptr socket);

  int           srv_port;
  std::string   ip_address;

  ios_ptr       ios;
  endpoint_ptr  endpoint;
  acceptor_ptr  acceptor;
  boost::thread_group comm_threads;
};

// Manage communication with Application
class AppCommManager : public CommManager 
{
  TEST_FRIENDS_LIST
public:
  AppCommManager(
      PlatformManager* _platform,
      std::string address = "127.0.0.1",
      int ip_port = 1027
    ): CommManager(_platform, address, ip_port, 24) {;}
private:
  void process(socket_ptr);
  void handleAccRegister(TaskMsg &msg);
  void handleAccDelete(TaskMsg &msg);
};

class AccReject : public std::logic_error {
public:
  explicit AccReject(const std::string& what_arg):
    std::logic_error(what_arg) {;}
};

class AccFailure : public std::logic_error {
public:
  explicit AccFailure(const std::string& what_arg):
    std::logic_error(what_arg) {;}
};

// Manager communication with GAM
class GAMCommManager : public CommManager 
{
  TEST_FRIENDS_LIST
public:
  GAMCommManager(
      PlatformManager* _platform,
      std::string address = "127.0.0.1",
      int ip_port = 1028
    ): CommManager(_platform, address, ip_port, 4) {;}
private:
  void process(socket_ptr);
  std::vector<std::pair<std::string, std::string> > last_labels;
};
} // namespace blaze
#endif
