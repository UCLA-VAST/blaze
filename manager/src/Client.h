#ifndef CLIENT_H
#define CLIENT_H

#include <cstdint>
#include <string>
#include <vector>
#include <stdexcept>

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/filesystem.hpp>

#include "proto/task.pb.h"
#include "Common.h"

#define BLAZE_INPUT         0
#define BLAZE_INPUT_CACHED  1
#define BLAZE_SHARED        2
#define BLAZE_OUTPUT        3

using namespace boost::asio;

namespace blaze {

typedef boost::shared_ptr<io_service> ios_ptr;
typedef boost::shared_ptr<ip::tcp::endpoint> endpoint_ptr;
typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

class Client {
  
public:
  Client(std::string _acc_id, 
      int port = 1027);

  // allocate a new block and return the pointer
  // NOTE: assuming the blocks are ordered
  void* alloc(int num_items, int item_length, int item_size, int type);

  // write data to existing block
  void writeBlock(int idx, void* src, size_t size);

  // read data from existing block 
  void readBlock(int idx, void* dst, size_t size);

  // get the pointer from a block
  void* getData(int idx);

  int getInputLength(int idx);

  int getInputNumItems(int idx);

  void start();

  // pure virtual method to be overloaded
  virtual void compute() = 0;

private:

  // helper functions in communication flow
  void prepareRequest(TaskMsg &msg);
  void prepareData(TaskMsg &data_msg, TaskMsg &reply_msg);
  void processOutput(TaskMsg &msg);

  void recv(TaskMsg&, socket_ptr);
  void send(TaskMsg&, socket_ptr);

  std::string acc_id;
  std::string app_id;

  // connection
  int srv_port;
  std::string ip_address;
  ios_ptr ios;
  endpoint_ptr endpoint;

  // input/output data blocks
  std::vector<std::pair<int64_t, DataBlock_ptr> > blocks;
  std::vector<bool> blocks_cached;
  int num_inputs;
  int num_outputs;
};

} // namespace blaze

#endif
