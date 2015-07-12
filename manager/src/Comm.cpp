#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <iostream>
#include <stdexcept>
#include <fcntl.h>   
#include <sys/mman.h>
#include <sys/stat.h>

#include "Comm.h"

#define MAX_MSGSIZE 4096
#define LOG_HEADER  std::string("Comm::") + \
                    std::string(__func__) +\
                    std::string("(): ")

namespace acc_runtime {

// receive one message, bytesize first
void Comm::recv(
    TaskMsg &task_msg, 
    ip::tcp::iostream &socket_stream) 
{
  int msg_size = 0;

  //TODO: why doesn't this work: socket_stream >> msg_size;
  socket_stream.read(reinterpret_cast<char*>(&msg_size), sizeof(int));

  if (msg_size<=0) {
    throw std::runtime_error(
        "Invalid message size of " +
        std::to_string((long long)msg_size));
  }

  char* msg_data = new char[msg_size];
  socket_stream.read(msg_data, msg_size);

  if (!task_msg.ParseFromArray(msg_data, msg_size)) {
    throw std::runtime_error("Failed to parse input message");
  }

  delete msg_data;
}

// send one message, bytesize first
void Comm::send(
    TaskMsg &task_msg, 
    ip::tcp::iostream &socket_stream) 
{
  int msg_size = task_msg.ByteSize();

  //TODO: why doesn't this work: socket_stream << msg_size;
  socket_stream.write(reinterpret_cast<char*>(&msg_size), sizeof(int));

  task_msg.SerializeToOstream(&socket_stream);
}

void Comm::process(socket_ptr sock) {

  // This may not be the best available method
  ip::tcp::iostream socket_stream;
  socket_stream.rdbuf()->assign( ip::tcp::v4(), sock->native());

  // log info
  std::string msg = 
    LOG_HEADER + 
    std::string("Start processing a new connection.");
  logger->logInfo(msg);

  TaskMsg task_msg;

  try {
    recv(task_msg, socket_stream);
  } catch (std::runtime_error &e) {
    std::string msg = LOG_HEADER + e.what();
    logger->logErr(msg);
    return;
  }

  if (task_msg.type() == ACCREQUEST) {

    logger->logInfo(
        LOG_HEADER + 
        std::string("Received an ACCREQUEST message."));

    TaskMsg reply_msg;

    // query the queue manager to find matching acc
    TaskManager_ptr task_manager = 
      queue_manager->get(task_msg.acc_id());

    if (task_manager == NULL_TASK_MANAGER) {
      // if there is no matching acc
      logger->logInfo(
          LOG_HEADER + 
          std::string("No matching accelerators, rejecting request"));

      reply_msg.set_type(ACCREJECT);

      // send msg back to client
      send(reply_msg, socket_stream);

      return;
    }

    // TODO: calculate scheduling decision
    // here assuming always accept
    reply_msg.set_type(ACCGRANT);

    // create a task, which will be automatically enqueued
    Task* task = task_manager->create();

    bool all_cached = true;

    // consult BlockManager to see if block is cached
    for(int i = 0; i < task_msg.data_size(); ++i) {
      int blockId = task_msg.data(i).partition_id();

      DataMsg *block_info = reply_msg.add_data();
      block_info->set_partition_id(blockId);

      if (!block_manager->isCached(blockId)) {

        // allocate a new block without initilizing
        // this block need to be added to cache later since the
        // size information may not be available at this point
        DataBlock_ptr block(new DataBlock());

        // add the block to task
        // this step may be mandatory to guarantee correct block
        // order
        task->addInputBlock(blockId, block);

        // set message flag
        block_info->set_cached(false);
        all_cached = false;
      }
      else {
        DataBlock_ptr block = block_manager->get(blockId);

        // add cached block to task
        task->addInputBlock(blockId, block);

        block_info->set_cached(false); 
      }
    }

    // send msg back to client
    send(reply_msg, socket_stream);

    logger->logInfo(
        LOG_HEADER + 
        std::string("Replied with an ACCGRANT message."));

    // wait for ACCDATA message if not all blocks are cached
    if (!all_cached) {

      TaskMsg data_msg;

      try {
        recv(data_msg, socket_stream);
      } catch (std::runtime_error &e) {
        std::string msg = LOG_HEADER + e.what();
        logger->logErr(msg);
        return;
      }

      // Acquire data from Spark
      if (data_msg.type() == ACCDATA) {

        // TaskManager.onDataReady(task_id, partition_id);
        for (int d = 0; d < data_msg.data_size(); ++d) {

          int blockId = data_msg.data(d).partition_id();
          int dataSize = data_msg.data(d).size();
          int dataLength = data_msg.data(d).length();
          std::string dataPath = data_msg.data(d).path();

          // get the updated block from task
          DataBlock_ptr block = 
            task->onDataReady(blockId, dataLength, dataSize, dataPath);

          // add the block to cache
          block_manager->add(blockId, block);
        }
      }
      else {
        std::string msg = 
          LOG_HEADER + 
          std::string("Unknown message type, discarding message.");
        logger->logErr(msg);
      }
    }

    // wait on task finish
    while (task->status != Task::FINISHED) {
      boost::this_thread::sleep_for(
          boost::chrono::microseconds(10)); 
    }

    logger->logInfo(
        LOG_HEADER + 
        std::string("Task finished"));

    // Initialize finish message
    TaskMsg finish_msg;
    finish_msg.set_type(ACCFINISH);

    // add block information to finish message 
    // for all output blocks
    DataBlock_ptr block;
    int outId = 0;

    while ((block = task->getOutputBlock()) != NULL_DATA_BLOCK) {

      // use thread id to create unique output file path
      std::string path = 
        "/tmp/" + 
        logger->getTid() + 
        std::to_string((long long)outId);

      logger->logInfo(
          LOG_HEADER + 
          std::string("Write output block to ") +
          path);

      // write the block to output shared memory
      block->writeToMem(path);

      // construct DataMsg
      DataMsg *block_info = finish_msg.add_data();
      block_info->set_partition_id(outId);
      block_info->set_path(path); 
      block_info->set_length(block->getLength());	
      block_info->set_size(block->getSize());	

      outId ++;
    }

    send(finish_msg, socket_stream);

    std::string msg = 
      LOG_HEADER + 
      std::string("Sent an ACCFINISH message.");
    logger->logInfo(msg);

  }
  else if (task_msg.type() == ACCBROADCAST) {

    logger->logInfo(LOG_HEADER + 
        std::string("Recieved a ACCBROADCAST message")) ; 

    bool success = true;
    for (int d=0; d< task_msg.data_size(); d++) {
      const DataMsg blockInfo = task_msg.data(d);
      int blockId = blockInfo.partition_id();

      if (blockInfo.has_length()) {
        // add a new block 
        std::string dataPath = blockInfo.path(); 
        int dataLength = blockInfo.length();
        int dataSize = blockInfo.size();	

        DataBlock_ptr block = block_manager->getShared(blockId);

        if (block == NULL_DATA_BLOCK) {
          // broadcast block does not exist
          // create a new block and add it to the manager

          // not sure smart_ptr can be assigned, so creating a new one
          DataBlock_ptr new_block(new DataBlock(dataLength, dataSize));

          new_block->readFromMem(dataPath);

          int err = block_manager->addShared(blockId, new_block);

          if (err != 0) { // not enough space
            logger->logErr(
                LOG_HEADER+
                "Not enough space left in scratch");

            success = false;
            break;
          }
        }
        else {
          // if the block already exists, update it
          block->readFromMem(dataPath);
        }
      }
      else {
        // deleting an existing block
        int err = block_manager->removeShared(blockId);

        if (err != 0) { // failed to remove
          success = false;  

          logger->logErr(
              LOG_HEADER+
              "Failed to delete the block");

          // do not break out of the loop
        }
      }
    }

    TaskMsg finish_msg;
    if (success) {
      finish_msg.set_type(ACCFINISH);
      logger->logInfo(
          LOG_HEADER+
          "Replied an ACCFINISH message regarding the broadcast.");
    }
    else {
      finish_msg.set_type(ACCFAILURE);
      logger->logInfo(
          LOG_HEADER+
          "Replied an ACCFAILURE message regarding the broadcast.");
    }
    send(finish_msg, socket_stream);
  }
  else {
    std::string msg = 
      LOG_HEADER + 
      std::string("Unknown message type, discarding message.");
    logger->logErr(msg);
  }
  logger->logInfo(
      LOG_HEADER+
      "thread exiting.");
}

void Comm::listen() {

  io_service ios;

  ip::tcp::endpoint endpoint(
      ip::address::from_string(ip_address),
      srv_port);

  ip::tcp::acceptor acceptor(ios, endpoint);

  logger->logInfo(LOG_HEADER + 
      std::string("start listening for new connections"));

  while(1) {

    // create socket for connection
    socket_ptr sock(new ip::tcp::socket(ios));

    // accept incoming connection
    acceptor.accept(*sock);
    //acceptor.accept(*socket_stream.rdbuf());

    std::string msg = 
      LOG_HEADER + 
      std::string("Accepted a new connection.");
    logger->logInfo(msg);

    boost::thread t(boost::bind(&Comm::process, this, sock));
  }
}

}

