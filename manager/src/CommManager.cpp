#include <stdio.h>
#include <time.h>
#include <fcntl.h>   
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <iostream>
#include <fstream>
#include <cstdint>

#define LOG_HEADER "CommManager"
#include <glog/logging.h>

#include "CommManager.h"
#include "PlatformManager.h"
#include "BlockManager.h"
#include "TaskManager.h"

#define MAX_MSGSIZE 4096

namespace blaze {

// receive one message, bytesize first
void CommManager::recv(
    ::google::protobuf::Message &msg, 
    socket_ptr socket) 
{
  try {
    int msg_size = 0;

    socket->receive(buffer(reinterpret_cast<char*>(&msg_size), sizeof(int)), 0);

    if (msg_size<=0) {
      throw std::runtime_error(
          "Invalid message size of " +
          std::to_string((long long)msg_size));
    }
    char* msg_data = new char[msg_size];

    socket->receive(buffer(msg_data, msg_size), 0);

    if (!msg.ParseFromArray(msg_data, msg_size)) {
      throw std::runtime_error("Failed to parse input message");
    }

    delete [] msg_data;
  } catch (std::exception &e) {
    throw std::runtime_error(e.what());
  }
}

// send one message, bytesize first
void CommManager::send(
    ::google::protobuf::Message &msg, 
    socket_ptr socket) 
{
  try {
    int msg_size = msg.ByteSize();

    //NOTE: why doesn't this work: socket_stream << msg_size;
    socket->send(buffer(reinterpret_cast<char*>(&msg_size), sizeof(int)),0);

    char* msg_data = new char[msg_size];

    msg.SerializeToArray(msg_data, msg_size);

    socket->send(buffer(msg_data, msg_size),0);
  } catch (std::exception &e) {
    throw std::runtime_error(e.what());
  }
}

void CommManager::listen() {

  try {
    io_service ios;

    ip::tcp::endpoint endpoint(
        ip::address::from_string(ip_address),
        srv_port);

    ip::tcp::acceptor acceptor(ios, endpoint);

    // start io service processing loop
    io_service::work work(ios);

    // create a thread pool
    boost::thread_group threadPool;

    for (int t=0; t<max_threads; t++) 
    {
      threadPool.create_thread(
          boost::bind(&io_service::run, &ios));
    }

    VLOG(2) << "Listening for new connections at "
      << ip_address << ":" << srv_port;

    while(1) {

      //boost::this_thread::sleep_for(boost::chrono::microseconds(100)); 

      // create socket for connection
      socket_ptr sock(new ip::tcp::socket(ios));

      // accept incoming connection
      acceptor.accept(*sock);

      //boost::thread t(boost::bind(&CommManager::process, this, sock));
      // post a job to a worker thread
      ios.post(boost::bind(&CommManager::process, this, sock));
    }
    ios.stop();
  }
  catch (std::exception &e) {
    // do not throw exception, just end current thread
    LOG(ERROR) << e.what();
  }
}
} // namespace blaze
