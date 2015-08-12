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

#include "Context.h"
#include "BlockManager.h"
#include "QueueManager.h"
#include "TaskManager.h"
#include "Logger.h"

using namespace boost::asio;

typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

namespace acc_runtime {

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
class Comm 
: public boost::basic_lockable_adapter<boost::mutex>
{
public:
  Comm(
      Context* _context,
      QueueManager* _queue_manager,
      Logger* _logger,
      std::string address = "127.0.0.1",
      int ip_port = 1027
    ):
    ip_address(address), 
    srv_port(ip_port), 
    context(_context),
    queue_manager(_queue_manager),
    logger(_logger)
  { 
    ;
  }

  void recv(TaskMsg&, ip::tcp::iostream&);

  void send(TaskMsg&, ip::tcp::iostream&);

  void process(socket_ptr); // processing messages

  void listen(); // always on kernel to wait for connection

  void addTask(std::string id);
  void removeTask(std::string id);
  
private:
  int srv_port;
  std::string ip_address;

  std::map<std::string, int> num_tasks;

  // reference to context
  Context *context;

  // reference to queue manager
  QueueManager *queue_manager;

  Logger *logger;
};
}

#endif
