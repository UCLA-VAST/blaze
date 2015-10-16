#ifndef BlazeClient_H
#define BlazeClient_H

#include <stdlib.h>
#include <time.h>
#include <string>
#include <vector>
#include <stdexcept>

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/filesystem.hpp>

#include "proto/task.pb.h"
#include "Block.h"
#include "Logger.h"

#define LOG_HEADER  std::string("BlazeClient::") + \
                    std::string(__func__) +\
                    std::string("(): ")

#define BLAZE_INPUT         0
#define BLAZE_INPUT_CACHED  1
#define BLAZE_OUTPUT        2

using namespace boost::asio;

namespace blaze {

typedef boost::shared_ptr<ip::tcp::iostream> iostream_ptr;
typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

class BlazeClient {
  
public:
  BlazeClient(std::string id, 
      int port = 1027,
      int verbose = 2):
    acc_id(id), 
    ip_address("127.0.0.1"),
    srv_port(port),
    num_inputs(0),
    num_outputs(0)
  {
    srand(time(NULL));

    logger = new Logger(verbose);

  }

  // allocate a new block and return the pointer
  // NOTE: assuming the blocks are ordered
  void* alloc(int data_width, int length, int type);

  // write data to existing block
  void writeBlock(int idx, void* src, size_t size);

  // read data from existing block 
  void readBlock(int idx, void* dst, size_t size);

  // get the pointer from a block
  void* getData(int idx) {
    return (void*)blocks[idx].second->getData();
  };

  void start();

private:

  // helper functions in communication flow
  void prepareRequest(TaskMsg &msg);
  void prepareData(TaskMsg &data_msg, TaskMsg &reply_msg);
  void processOutput(TaskMsg &msg);

  void recv(TaskMsg&);
  void send(TaskMsg&);

  // accelerator id
  std::string acc_id;

  // connection
  int srv_port;
  std::string ip_address;
  iostream_ptr socket_stream;
  socket_ptr  socket;

  // input/output data blocks
  std::vector<std::pair<int, DataBlock_ptr> > blocks;
  std::vector<bool> blocks_cached;
  int num_inputs;
  int num_outputs;

  // logger
  Logger *logger;
};


void* BlazeClient::alloc(int data_width, int length, int type) {
  
  DataBlock_ptr block(new DataBlock(length, data_width*length));
  blocks.push_back(std::make_pair(blocks.size(), block));

  if (type==BLAZE_INPUT) {
    num_inputs ++;
    blocks_cached.push_back(false);
  }
  else if (type==BLAZE_INPUT_CACHED) {
    num_inputs ++;
    blocks_cached.push_back(true);
  }
  else if (type==BLAZE_OUTPUT) {
    num_outputs ++;
  }

  return block->getData();
}

void BlazeClient::writeBlock(int idx, void* src, size_t size) {
  if (idx >= blocks.size()) {
    return;
  }
  DataBlock_ptr block = blocks[idx].second;
  block->writeData(src, size);
}

void BlazeClient::readBlock(int idx, void* dst, size_t size) {
  if (idx >= blocks.size()) {
    return;
  }
  DataBlock_ptr block = blocks[idx].second;
  block->readData(dst, size);
}

void BlazeClient::start() {
  
  io_service ios;

  // setup socket connection
  ip::tcp::endpoint endpoint(
      ip::address::from_string(ip_address),
      srv_port);

  // create socket for connection
  socket_ptr sock(new ip::tcp::socket(ios));
  sock->connect(endpoint);
  sock->set_option(ip::tcp::no_delay(true));

  socket = sock;

  try {

    // send request
    TaskMsg request_msg;
    prepareRequest(request_msg);
    send(request_msg);

    logger->logInfo(LOG_HEADER+std::string("Sent a request"));

    // wait on reply for ACCREQUEST
    TaskMsg reply_msg;
    recv(reply_msg);

    if (reply_msg.type() == ACCGRANT) {

      TaskMsg data_msg;
      prepareData(data_msg, reply_msg);
      send(data_msg);
      logger->logInfo(LOG_HEADER+std::string("Sent data"));
    }
    else {
      throw std::runtime_error("request rejected");
    }

    TaskMsg finish_msg;
    // wait on reply for ACCDATA
    recv(finish_msg);

    if (finish_msg.type() == ACCFINISH) {
      processOutput(finish_msg);
    }
    else {
      throw std::runtime_error("did not receive ACCFINISH");
    }
     
  }
  catch (std::exception &e) {
    logger->logErr(LOG_HEADER+
        std::string("Task failed because:")+
        e.what());
  }
}

void BlazeClient::prepareRequest(TaskMsg &msg) {

  msg.set_type(ACCREQUEST);
  msg.set_acc_id(acc_id);

  for (int i=0; i<num_inputs; i++) {
    DataMsg *block_info = msg.add_data();
    block_info->set_partition_id(blocks[i].first);
  }

  logger->logInfo(LOG_HEADER+
      std::string("Requesting accelerator ")+
      acc_id);
}

void BlazeClient::prepareData(TaskMsg &data_msg, TaskMsg &reply_msg) {
 
  data_msg.set_type(ACCDATA);
  
  logger->logInfo(LOG_HEADER+
      std::string("Start writing data to memory"));

  for (int i=0; i<num_inputs; i++) {

    if (!reply_msg.data(i).cached()) {

      DataMsg *block_info = data_msg.add_data();
      block_info->set_partition_id(blocks[i].first);

      // write data to memory mapped file
      // use thread id to create unique output file path
      std::string path = 
        "/tmp/" + 
        logger->getTid() + 
        std::to_string((long long)blocks[i].first);

      DataBlock_ptr block = blocks[i].second;
      block->writeToMem(path);

      block_info->set_path(path);
      block_info->set_length(block->getLength());
      block_info->set_size(block->getSize());
      block_info->set_num_items(block->getNumItems());

      logger->logInfo(LOG_HEADER+
          std::string("Finish writing block ")+
          std::to_string((long long) i));
    }
    // to guarantee the id is unique each time
    if (!blocks_cached[i]) {
      blocks[i].first += blocks.size();
    }
  }
}

void BlazeClient::processOutput(TaskMsg &msg) {

  logger->logInfo(LOG_HEADER+
      std::string("Task finished, start reading output"));

  if (num_outputs != msg.data_size()) {
    throw std::runtime_error("Failed to process output");
  }

  for (int i=0; i<msg.data_size(); i++) {
    DataMsg block_info = msg.data(i);

    std::string path = block_info.path();
    try {
      blocks[num_inputs + i].second->readFromMem(path);

      float* fdata = (float*)(blocks[num_inputs + i].second->getData());

      // delete memory map file after read
      boost::filesystem::wpath file(path);
      if (boost::filesystem::exists(file)) {
        boost::filesystem::remove(file);
      }
    }
    catch (std::runtime_error &e) {
      logger->logErr(LOG_HEADER + 
          std::string("Failed to read output block ")+
          std::to_string((long long)i));
      throw std::runtime_error("Failed to process output");
    }
  }
  logger->logInfo(LOG_HEADER+
      std::string("Finish reading output blocks"));
}

void BlazeClient::recv(TaskMsg &task_msg)
{
  int msg_size = 0;

  socket->receive(buffer(reinterpret_cast<char*>(&msg_size), sizeof(int)), 0);

  if (msg_size<=0) {
    throw std::runtime_error(
        "Invalid message size of " +
        std::to_string((long long)msg_size));
  }

  char* msg_data = new char[msg_size];
  socket->receive(buffer(msg_data, msg_size), 0);

  if (!task_msg.ParseFromArray(msg_data, msg_size)) {
    throw std::runtime_error("Failed to parse input message");
  }

  delete msg_data;
}

// send one message, bytesize first
void BlazeClient::send(TaskMsg &task_msg)
{
  int msg_size = task_msg.ByteSize();

  socket->send(buffer(reinterpret_cast<char*>(&msg_size), sizeof(int)),0);

  char* msg_data = new char[msg_size];

  task_msg.SerializeToArray(msg_data, msg_size);

  socket->send(buffer(msg_data, msg_size),0);
}

} // namespace blaze

#endif
