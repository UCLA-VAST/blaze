#ifndef COMM_H
#define COMM_H

#include <string>
#include <vector>

#include <boost/smart_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include <google/protobuf/message.h>

#include "proto/task.pb.h"
#include "Common.h"

using namespace boost::asio;
typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

namespace blaze {

/*
 * Communicator design for Node Manager
 */
class CommManager
: public boost::basic_lockable_adapter<boost::mutex>
{
public:
  CommManager(
      PlatformManager* _platform,
      std::string address = "127.0.0.1",
      int ip_port = 1027,
      int _max_threads = boost::thread::hardware_concurrency()
    ):
    ip_address(address), 
    srv_port(ip_port), 
    platform_manager(_platform),
    max_threads(_max_threads)
  { 
    // asynchronously start listening for new connections
    boost::thread t(boost::bind(&CommManager::listen, this));
  }

protected:
  void recv(::google::protobuf::Message&, socket_ptr);

  void send(::google::protobuf::Message&, socket_ptr);

  // pure virtual method called by listen
  virtual void process(socket_ptr) {};

  // reference to platform manager
  PlatformManager *platform_manager;

private:
  void listen();

  int max_threads;
  int srv_port;
  std::string ip_address;
};

// Manage communication with Application
class AppCommManager : public CommManager 
{
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


}

#endif
