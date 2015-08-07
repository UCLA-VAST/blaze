#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <fcntl.h>   
#include <sys/mman.h>
#include <sys/stat.h>

#include <iostream>
#include <stdexcept>
#include <cstdint>

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

  //NOTE: why doesn't this work: socket_stream << msg_size;
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

    // get correponding block manager based on platform context
    BlockManager* block_manager = context->
      getBlockManager(task_msg.acc_id());

    // create a task, which will be automatically enqueued
    Task* task = task_manager->create();

    bool all_cached = true;

    try {
      // consult BlockManager to see if each block is cached
      for (int i = 0; i < task_msg.data_size(); ++i) {
        int64_t blockId = task_msg.data(i).partition_id();

        DataMsg *block_info = reply_msg.add_data();
        block_info->set_partition_id(blockId);

        DataBlock_ptr block;

        //TODO: issue with multithread access,
        // both threads will see the block is not cached,
        // so they will all set cached = false; but only one
        // need to
        if (block_manager->contains(blockId)) {
          // get block from BlockManager
          block = block_manager->get(blockId);

          // set message flag
          block_info->set_cached(true); 
        }
        else {
          if (blockId >= 0) { // this is an input block

            // allocate a new block without initilizing
            // this block need to be added to cache later since the
            // size information may not be available at this point
            block = block_manager->create();
            
            block_info->set_cached(false); 
            // set message flag
            all_cached = false;
          }
          else { // this is a broadcast block

            // create the block and add it to scratch
            // NOTE: at this point multiple threads may try 
            // to add the same block, so create() is locked
            bool created = block_manager->create(blockId, block);

            if (created) {
              logger->logInfo(LOG_HEADER+
                  std::to_string(blockId)+
                  " not cached");
            
              block_info->set_cached(false); 
              all_cached = false;
            }
            else {
              logger->logInfo(LOG_HEADER+
                  std::to_string(blockId)+
                  " cached");
              block_info->set_cached(true); 
            }
            // the block added to task allocated but not ready
            // at this point
          }
        }
        // add block to task
        task->addInputBlock(blockId, block);
      }

      // send msg back to client
      send(reply_msg, socket_stream);

      logger->logInfo(
          LOG_HEADER + 
          std::string("Replied with an ACCGRANT message."));
    }
    catch (std::runtime_error &e) {

      // send ACCFAILURE to client
      TaskMsg reply_msg;
      reply_msg.set_type(ACCFAILURE);

      send(reply_msg, socket_stream);

      logger->logErr(
          LOG_HEADER + 
          std::string("Exception caught during processing request: ")+
          e.what()+
          ", send ACCFAILURE");
      return;
    }

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

        for (int d = 0; d < data_msg.data_size(); ++d) {

          const DataMsg blockInfo = data_msg.data(d);
          int64_t blockId = blockInfo.partition_id();

          logger->logInfo(LOG_HEADER+
              "start reading data for block "+
              std::to_string(blockId));

          try {
            // get the updated block from task
            DataBlock_ptr block = 
              task->onDataReady(blockInfo);

            logger->logInfo(LOG_HEADER+
                "finish reading data for block "+
                std::to_string(blockId));

            if (blockId >= 0) {
              // add the block to cache
              block_manager->add(blockId, block);
            }
          } catch ( std::runtime_error &e ) {

            logger->logErr(
                LOG_HEADER + 
                std::string("Error receiving data of block ") +
                std::to_string((long long)blockId) +
                std::string(" ") + std::string(e.what()));
            break;
          }
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
    while (
        task->status != Task::FINISHED && 
        task->status != Task::FAILED) 
    {
      boost::this_thread::sleep_for(
          boost::chrono::microseconds(10)); 
    }

    // Initialize finish message
    TaskMsg finish_msg;

    bool task_finished = true;

    if (task->status == Task::FINISHED) {

      // add block information to finish message 
      // for all output blocks
      int64_t outId = 0;
      DataBlock_ptr block;
      bool block_left = true;

      // NOTE: there should not be more than one block
      while (block_left)  {

        block_left = task->getOutputBlock(block);

        // use thread id to create unique output file path
        std::string path = 
          "/tmp/" + 
          logger->getTid() + 
          std::to_string((long long)outId);

        try {
          // write the block to output shared memory
          block->writeToMem(path);
        } catch ( std::runtime_error &e ) {
          task_finished = false;
          logger->logErr(LOG_HEADER + 
              std::string("writeToMem error: ") +
              e.what());

          break;
        }

        logger->logInfo(
            LOG_HEADER + 
            std::string("Write output block to ") +
            path);

        // construct DataMsg
        DataMsg *block_info = finish_msg.add_data();
        block_info->set_partition_id(outId);
        block_info->set_path(path); 
        block_info->set_length(block->getLength());	
        block_info->set_num_items(block->getNumItems());	
        block_info->set_size(block->getSize());	

        outId ++;
      }
    }
    else {
      task_finished = false;
    }

    if (task_finished) {
      finish_msg.set_type(ACCFINISH);

      std::string msg = 
        LOG_HEADER + 
        std::string("Task finished, sent an ACCFINISH.");
      logger->logInfo(msg);
    }
    else {
      finish_msg.set_type(ACCFAILURE);

      std::string msg = 
        LOG_HEADER + 
        std::string("Task failed, sent an ACCFAILURE.");
      logger->logInfo(msg);
    }

    send(finish_msg, socket_stream);
  }
  else if (task_msg.type() == ACCBROADCAST) {

    // NOTE: 
    // In this implementation, ACCBROADCAST is used only for 
    // removing the broadcast block for an application

    logger->logInfo(LOG_HEADER + 
        std::string("Recieved an ACCBROADCAST message")) ; 

    TaskMsg finish_msg;
    try {
      for (int d=0; d< task_msg.data_size(); d++) {
        const DataMsg blockInfo = task_msg.data(d);
        int64_t blockId = blockInfo.partition_id();

        // deleting an existing blocks from all block manager
        context->removeShared(blockId);
      }

      finish_msg.set_type(ACCFINISH);
      logger->logInfo(
          LOG_HEADER+
          "Replied an ACCFINISH message regarding the broadcast.");
    }
    catch (std::runtime_error &e) { 
      logger->logErr(
          LOG_HEADER+
          e.what());

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

