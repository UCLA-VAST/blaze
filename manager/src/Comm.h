#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "task.pb.h"

using namespace boost::asio;

typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

/*
 * Communicator design for Acc_Manager
 *
 */

namespace acc_runtime {
class Comm {

public:
  Comm(int ip_port): srv_port(ip_port) {;}
  Comm(std::string address, int ip_port): 
    ip_address(address), srv_port(ip_port) {;}
  
  void recv(TaskMsg&, ip::tcp::iostream&);
  void send(TaskMsg&, ip::tcp::iostream&);
  void process(socket_ptr); // processing messages
  void listen(); // always on kernel to wait for connection

private:
  int srv_port;
  std::string ip_address;
  std::vector<boost::thread> thread_pool;

  // reference to block manager
  // reference to task queue
};
}
