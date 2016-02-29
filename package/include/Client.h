#ifndef CLIENT_H
#define CLIENT_H

#include "proto/task.pb.h"
#include "Common.h"

#define BLAZE_INPUT         0
#define BLAZE_INPUT_CACHED  1
#define BLAZE_SHARED        2

// for testing purpose
#ifndef TEST_FRIENDS_LIST
#define TEST_FRIENDS_LIST
#endif

using namespace boost::asio;

namespace blaze {

typedef boost::shared_ptr<boost::thread> client_event;

class Client {
  TEST_FRIENDS_LIST
public:
  Client(std::string _acc_id, 
         int _num_inputs, 
         int _num_outputs,
         int port = 1027);

  void* createInput( int idx,
                     int num_items, 
                     int item_length, 
                     int data_width, 
                     int type = BLAZE_INPUT);

  void* createOutput(int idx,
                     int num_items, 
                     int item_length, 
                     int data_width);

  // copy data to an input block from a pointer,
  // allocate the space if the block has not been created
  void setInput(int idx, void* src, 
                int num_items = 0,
                int item_length = 0, 
                int data_width = 0,
                int type = BLAZE_INPUT);

  // get the data pointer and information of an input block
  void* getInputPtr(int idx);
  int   getInputNumItems(int idx);
  int   getInputLength(int idx);

  // get the data pointer and information of an output block
  void* getOutputPtr(int idx);
  int   getOutputNumItems(int idx);
  int   getOutputLength(int idx);

  // start client and wait for results
  // NOTE: in current version the call is blocking no matter
  // what input is provided
  void start(bool blocking = true);

  // pure virtual method to be overloaded
  virtual void compute() = 0;

private:
  // helper functions in communication flow
  void prepareRequest(TaskMsg &msg);
  void prepareData(TaskMsg &data_msg, TaskMsg &reply_msg);
  void processOutput(TaskMsg &msg);

  // routine function for socket communication
  //void recv(TaskMsg&, socket_ptr);
  //void send(TaskMsg&, socket_ptr);

  std::string acc_id;
  std::string app_id;

  // data structures for socket connection
  int srv_port;
  std::string ip_address;
  ios_ptr ios;
  endpoint_ptr endpoint;

  // input/output data blocks
  int num_inputs;
  int num_outputs;
  std::vector<DataBlock_ptr> input_blocks;
  std::vector<DataBlock_ptr> output_blocks;
  std::vector<std::pair<int64_t, bool> > block_info;
};

} // namespace blaze

#endif
