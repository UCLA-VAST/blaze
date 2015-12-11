#define LOG_HEADER  std::string("Client::") + \
                    std::string(__func__) +\
                    std::string("(): ")

#include "Block.h"
#include "Client.h"

namespace blaze {

Client::Client(
    std::string _acc_id, 
    std::string _app_id, 
    int port, int verbose):
  acc_id(_acc_id), 
  app_id(_app_id), 
  ip_address("127.0.0.1"),
  srv_port(port),
  num_inputs(0),
  num_outputs(0)
{
  srand(time(NULL));

  // setup socket connection
  ios_ptr _ios(new io_service);
  endpoint_ptr _endpoint(new ip::tcp::endpoint(
        ip::address::from_string(ip_address),
        srv_port));

  ios = _ios;
  endpoint = _endpoint;
}

void* Client::getData(int idx) {
    return (void*)blocks[idx].second->getData();
}

void* Client::alloc(
    int num_items, 
    int item_length, 
    int item_size, 
    int type) {
  
  DataBlock_ptr block(new DataBlock(num_items, item_length, item_size));

  int64_t block_id = 0;
  if (type==BLAZE_INPUT) {
    block_id = ((int64_t)getTid()<<10) + blocks.size();

    num_inputs ++;
    blocks_cached.push_back(false);
  }
  else if (type==BLAZE_INPUT_CACHED) {
    block_id = ((int64_t)getTid()<<10) + blocks.size();

    num_inputs ++;
    blocks_cached.push_back(true);
  }
  else if (type==BLAZE_SHARED) {
    block_id = ((int64_t)getTid()<<10) + blocks.size();
    block_id = 0 - block_id; // broadcast id is negative
    
    num_inputs++;
    blocks_cached.push_back(false);
  }
  else if (type==BLAZE_OUTPUT) {
    num_outputs ++;
  }

  blocks.push_back(std::make_pair(block_id, block));
  return block->getData();
}

void Client::writeBlock(int idx, void* src, size_t size) {
  if (idx >= blocks.size()) {
    return;
  }
  DataBlock_ptr block = blocks[idx].second;
  block->writeData(src, size);
}

void Client::readBlock(int idx, void* dst, size_t size) {
  if (idx >= blocks.size()) {
    return;
  }
  DataBlock_ptr block = blocks[idx].second;
  block->readData(dst, size);
}

void Client::start() {

  // create socket for connection
  socket_ptr sock(new ip::tcp::socket(*ios));
  sock->connect(*endpoint);
  sock->set_option(ip::tcp::no_delay(true));

  try {

    // send request
    TaskMsg request_msg;
    prepareRequest(request_msg);
    send(request_msg, sock);

    logInfo(LOG_HEADER+std::string("Sent a request"));

    // wait on reply for ACCREQUEST
    TaskMsg reply_msg;
    recv(reply_msg, sock);

    if (reply_msg.type() == ACCGRANT) {

      TaskMsg data_msg;
      prepareData(data_msg, reply_msg);
      send(data_msg, sock);
      logInfo(LOG_HEADER+std::string("Sent data"));
    }
    else {
      throw std::runtime_error("request rejected");
    }

    TaskMsg finish_msg;
    // wait on reply for ACCDATA
    recv(finish_msg, sock);

    if (finish_msg.type() == ACCFINISH) {
      processOutput(finish_msg);
    }
    else {
      throw std::runtime_error("did not receive ACCFINISH");
    }
     
  }
  catch (std::exception &e) {
    logErr(LOG_HEADER+
        std::string("Task failed because:")+
        e.what());
  }
}

void Client::prepareRequest(TaskMsg &msg) {

  msg.set_type(ACCREQUEST);
  msg.set_acc_id(acc_id);
  msg.set_app_id(app_id);

  for (int i=0; i<num_inputs; i++) {
    DataMsg *block_info = msg.add_data();
    
    // check if the data is scalar
    if (blocks[i].second->getNumItems() == 1 && 
        blocks[i].second->getItemLength() == 1)
    {
      char* data = blocks[i].second->getData();
      block_info->set_scalar_value(*((long long*)data));
    }
    else {
      block_info->set_partition_id(blocks[i].first);
      if (!blocks_cached[i]) {
        block_info->set_cached(false);
      }
    }
  }

  logInfo(LOG_HEADER+
      std::string("Requesting accelerator ")+
      acc_id);
}

void Client::prepareData(TaskMsg &data_msg, TaskMsg &reply_msg) {
 
  data_msg.set_type(ACCDATA);
  
  logInfo(LOG_HEADER+
      std::string("Start writing data to memory"));

  for (int i=0; i<reply_msg.data_size(); i++) {

    if (!reply_msg.data(i).cached()) {

      DataMsg *block_info = data_msg.add_data();
      block_info->set_partition_id(blocks[i].first);

      // write data to memory mapped file
      // use thread id to create unique output file path
      std::string path = "/tmp/" + 
            boost::lexical_cast<std::string>(boost::this_thread::get_id())+
            std::to_string((long long)i);

      DataBlock_ptr block = blocks[i].second;
      block->writeToMem(path);

      block_info->set_file_path(path);
      block_info->set_num_elements(block->getNumItems());
      block_info->set_element_length(block->getItemLength());
      block_info->set_element_size(block->getItemSize());

      logInfo(LOG_HEADER+
          std::string("Finish writing block ")+
          std::to_string((long long) i));
    }
  }
}

void Client::processOutput(TaskMsg &msg) {

  logInfo(LOG_HEADER+
      std::string("Task finished, start reading output"));

  if (num_outputs != msg.data_size()) {
    throw std::runtime_error("Failed to process output");
  }

  for (int i=0; i<msg.data_size(); i++) {
    DataMsg block_info = msg.data(i);

    std::string path = block_info.file_path();
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
      logErr(LOG_HEADER + 
          std::string("Failed to read output block ")+
          std::to_string((long long)i));
      throw std::runtime_error("Failed to process output");
    }
  }
  logInfo(LOG_HEADER+
      std::string("Finish reading output blocks"));
}

void Client::recv(TaskMsg &task_msg, socket_ptr socket)
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
void Client::send(TaskMsg &task_msg, socket_ptr socket)
{
  int msg_size = task_msg.ByteSize();

  socket->send(buffer(reinterpret_cast<char*>(&msg_size), sizeof(int)),0);

  char* msg_data = new char[msg_size];

  task_msg.SerializeToArray(msg_data, msg_size);

  socket->send(buffer(msg_data, msg_size),0);
}
} // namespace blaze
