#ifndef COMM_H
#define COMM_H

#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include "task.pb.h"
#include "acc_conf.pb.h"

#include "PlatformManager.h"
#include "BlockManager.h"
#include "TaskManager.h"
#include "Logger.h"

using namespace boost::asio;

typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

namespace blaze {

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

/*
 * Communicator design for Acc_Manager
 */
class CommManager
: public boost::basic_lockable_adapter<boost::mutex>
{
public:
  CommManager(
      PlatformManager* _platform,
      Logger* _logger,
      std::string address = "127.0.0.1",
      int ip_port = 1027
    ):
    ip_address(address), 
    srv_port(ip_port), 
    platform_manager(_platform),
    logger(_logger)
  { 
    // asynchronously start listening for new connections
    boost::thread t(boost::bind(&CommManager::listen, this));
  }

  void addTask(std::string id);
  void removeTask(std::string id);
  
private:
  void recv(TaskMsg&, socket_ptr);
  void send(TaskMsg&, socket_ptr);

  // processing messages
  void process(socket_ptr);

  void listen();

  int srv_port;
  std::string ip_address;

  std::map<std::string, int> num_tasks;

  // reference to platform manager
  PlatformManager *platform_manager;

  Logger *logger;
};
}

#endif
