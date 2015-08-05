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

    // consult BlockManager to see if block is cached
    for (int i = 0; i < task_msg.data_size(); ++i) {
      int64_t blockId = task_msg.data(i).partition_id();

      DataMsg *block_info = reply_msg.add_data();
      block_info->set_partition_id(blockId);

      if (blockId >= 0) { // this is an input block
        if (!block_manager->isCached(blockId)) {

          // allocate a new block without initilizing
          // this block need to be added to cache later since the
          // size information may not be available at this point
          DataBlock_ptr block = block_manager->create();

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

          block_info->set_cached(true); 
        }
      }
      else {
        DataBlock_ptr block = 
          block_manager->getShared(blockId);
        if (block == NULL_DATA_BLOCK) {
          // TODO: assuming broadcast data will
          // always be available?
          logger->logErr(
              LOG_HEADER + 
              std::string("Broadcast is not available"));
          // TODO: here should throw some exception
        }
        else {
          // add cached block to task
          task->addInputBlock(blockId, block);

          block_info->set_cached(true); 
        }
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

        for (int d = 0; d < data_msg.data_size(); ++d) {

          int64_t blockId = data_msg.data(d).partition_id();
          int dataLength = data_msg.data(d).length();
          int numItems = data_msg.data(d).has_num_items() ? 
                          data_msg.data(d).num_items() : 1;
          int64_t dataSize = data_msg.data(d).size();
          int64_t dataOffset = data_msg.data(d).offset();
          std::string dataPath = data_msg.data(d).path();

          //logger->logInfo(
          //    LOG_HEADER + 
          //    std::string("Received a block infomation of ")+
          //    std::to_string((long long)blockId));

          if (blockId < 0) {
            // should not do anything here assuming broadcast
            // data will be initialized by only ACCBROADCAST
            continue;
          }
          else {

            try {
              // get the updated block from task
              DataBlock_ptr block = 
                task->onDataReady(
                    blockId, 
                    dataLength, numItems,
                    dataSize, dataOffset,
                    dataPath);

              // add the block to cache
              block_manager->add(blockId, block);

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
        logger->logInfo("1 here");
      }
      logger->logInfo("3 here");
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
    // In this implementation, create the same broadcast variable 
    // in all block managers

    logger->logInfo(LOG_HEADER + 
        std::string("Recieved an ACCBROADCAST message")) ; 

    bool success = true;
    for (int d=0; d< task_msg.data_size(); d++) {
      const DataMsg blockInfo = task_msg.data(d);
      int64_t blockId = blockInfo.partition_id();

      if (blockInfo.has_length()) { // if this is a broadcast array

        // add a new block 
        std::string dataPath = blockInfo.path(); 
        int dataLength = blockInfo.length();
        int64_t dataSize = blockInfo.size();	

        // check all block managers
        DataBlock_ptr block = context->getShared(blockId);

        if (block == NULL_DATA_BLOCK) {
          // broadcast block does not exist
          // create a new block and add it to the manager

          // not sure smart_ptr can be assigned, so creating a new one
          DataBlock_ptr new_block(new DataBlock(dataLength, dataSize));

          // read block to CPU first
          new_block->readFromMem(dataPath);

          //int err = block_manager->addShared(blockId, new_block);
          try {
            context->addShared(blockId, new_block);
          }
          catch (std::runtime_error &e) { 
            logger->logErr(
                LOG_HEADER+
                e.what());

            success = false;
            break;
          }
        }
        else {
          // if the block already exists, update it
          block->readFromMem(dataPath);
        }
      }
      else if (blockInfo.has_bval()) { // if this is a broadcast scalar

        int64_t bval = blockInfo.bval();

        DataBlock_ptr block = context->getShared(blockId);

        if (block == NULL_DATA_BLOCK) {
          // broadcast block does not exist
          // create a new block and add it to the manager

          // add the scalar as a new block
          DataBlock_ptr new_block(new DataBlock(1, 8));

          new_block->writeData((void*)(&bval), 8);

          //int err = block_manager->addShared(blockId, new_block);
          try {
            context->addShared(blockId, new_block);
          }
          catch (std::runtime_error &e) { 
            logger->logErr(
                LOG_HEADER+
                e.what());

            success = false;
            break;
          }
        }
        else {
          // if the block already exists, update it
          block->writeData((void*)(&bval), 8);
        }
      }
      else {
        // deleting an existing blocks from all block manager
        try {
          context->removeShared(blockId);
        }
        catch (std::runtime_error &e) { 
          logger->logErr(
              LOG_HEADER+
              e.what());

          success = false;
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

