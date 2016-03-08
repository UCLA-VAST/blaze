#include <fstream>
#include <stdexcept>
#include <cstdint>

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>

#define LOG_HEADER "AppCommManager"

#include "proto/acc_conf.pb.h"
#include "Block.h"
#include "BlockManager.h"
#include "CommManager.h"
#include "Platform.h"
#include "PlatformManager.h"
#include "Task.h"
#include "TaskManager.h"

namespace blaze {

void AppCommManager::process(socket_ptr sock) {

  // turn off Nagle Algorithm to improve latency
  sock->set_option(ip::tcp::no_delay(true));

  // set socket buffer size to be 4MB
  socket_base::receive_buffer_size option(4*1024*1024);
  sock->set_option(option); 
  
  try {
    // 1. Handle ACCREQUEST
    TaskMsg task_msg;
    TaskMsg reply_msg;

    std::string app_id;
    std::string acc_id;

    // a table containing information of each input block
    // - partition_id: cached, sampled
    std::map<int64_t, std::pair<bool, bool> > block_table;

    // TODO: better naming
    // a table containing cache information of each input block
    // - partition_id: enable cache
    std::map<int64_t, bool> cache_table;

    try {
      recv(task_msg, sock);
    }
    catch (std::exception &e){
      throw AccFailure(
          std::string("Error in receiving TaskMsg: ")+
          std::string(e.what()));
    }
    if (task_msg.type() == ACCREQUEST) {

      if (!task_msg.has_acc_id() || !task_msg.has_app_id()) {
        throw AccReject("Missing acc_id or app_id");
      }
      acc_id = task_msg.acc_id();
      app_id = task_msg.app_id();

      LOG(INFO) << "Received an request for " << acc_id
        << " from app " << app_id;

      /* Receive acc_id to identify the corresponding TaskManager, 
       * with which create a new Task. 
       * Check the list of DataMsg to register input data blocks in the 
       * task's input table, in order to get the correct order
       */

      // query the PlatformManager to find matching acc
      if (!platform_manager->accExists(acc_id)) {
        throw AccReject("No matching accelerator"); 
      }

      // get correponding block manager based on platform of queried Task
      Platform* platform = platform_manager->getPlatformByAccId(
          task_msg.acc_id());

      // NOTE: ommit pointer check since it is 
      // already performed in getTaskManager

      BlockManager* block_manager = platform->getBlockManager();

      if (!block_manager) {
        throw AccReject("Cannot find block manager");
      }

      // create a new task, and make scheduling decision
      Task_ptr task;

      TaskManager_ref task_manager = 
        platform_manager->getTaskManager(task_msg.acc_id());

      // check if task_manager is still valid
      if (!task_manager.lock()) {
        // accelerator already removed
        DLOG(WARNING) << "Accelerator queue for " << acc_id
                      << " is likely to be removed";
        throw AccReject("No matching accelerator"); 
      }
      else {
        task = task_manager.lock()->create();
      }
      
      bool wait_accdata = false;

      // 1.3 iterate through each input block
      for (int i = 0; i < task_msg.data_size(); i++) {

        DataMsg recv_block = task_msg.data(i);

        // 1.3.1 if input is a scalar
        if (recv_block.has_scalar_value()) { 

          int64_t scalar_val = recv_block.scalar_value();

          // add the scalar as a new block
          DataBlock_ptr block(new DataBlock(1, 1, 8));
          block->writeData((void*)&scalar_val, 8);

          /* skip BlockManager and directly add it to task
           * NOTE: use naive block id here, since the id is only
           * used to differentiate the blocks in a single task
           */
          task->addInputBlock(i, block);

          DLOG(INFO) << "Received an scalar value";
        }
        // 1.3.2 if this is not a scalar, then its an array
        else {

          // check message schematic
          if (!recv_block.has_partition_id())
          {
            throw AccFailure("Missing partition_id in ACCREQUEST");
          }
          int64_t blockId = recv_block.partition_id();
          
          // reply entry in ACCGRANT
          DataMsg *reply_block = reply_msg.add_data();
          reply_block->set_partition_id(blockId);

          // reference block to add to the task's input table
          DataBlock_ptr block;

          // 1.3.2.1 if the input is a broadcast block
          if (blockId < 0) {

            int num_elements    = recv_block.num_elements();
            int element_length  = recv_block.has_element_length() ? 
                                  recv_block.element_length() : 0;
            int element_size    = recv_block.has_element_size() ? 
                                  recv_block.element_size() : 0;

            // check if the sizes are valid
            if (num_elements > 0 && (element_length <=0 || element_size <=0)) {
              throw AccFailure("Invalid block size info in ACCREQUEST");
            }

            if (block_manager->contains(blockId)) {

              block = block_manager->get(blockId);
              reply_block->set_cached(true); 
            }
            else {

              /* create a new empty block and add it to broadcast cache
               * NOTE: at this point multiple threads may try 
               * to create the same block, only one can be successful
               */
              bool created = block_manager->getAlloc(blockId, block,
                  num_elements, element_length, element_size);

              /* the return boolean indicates whether this is the 
               * task successfully created the block, and it will be
               * responsible of initializing the block
               */
              if (created) {
                reply_block->set_cached(false); 

                // wait for data in ACCDATA 
                wait_accdata = true; 
              }
              else {
                reply_block->set_cached(true); 
              }
            }
          }
          // 1.3.2.2 if the input is a normal input block
          else {
            // if the input block is non-cachable
            if (recv_block.has_cached() && !recv_block.cached()) {

              wait_accdata = true;

              // NOTE: do not support sampling at this point
              reply_block->set_cached(false); 
              reply_block->set_sampled(false);

              block = NULL_DATA_BLOCK;

              DLOG(INFO) << "Add a non-cachable block to task, id=" << blockId;

              // mark the block to skip cache
              cache_table.insert(std::make_pair(blockId, false));
            }
            else {
              DLOG(INFO) << "Add a cachable block to task, id=" << blockId;

              if (block_manager->contains(blockId)) {

                if (recv_block.has_sampled() && recv_block.sampled()) {

                  // wait for mask in ACCDATA 
                  wait_accdata = true; 

                  reply_block->set_sampled(true);
                }
                else {
                  block = block_manager->get(blockId);
                  reply_block->set_sampled(false);
                }
                reply_block->set_cached(true); 
              }
              // 1.3.2.2.2 if the input block is not cached
              else {

                // do not add block to task input table if data is sampled
                block = NULL_DATA_BLOCK;

                if (recv_block.has_sampled() && recv_block.sampled()) {

                  reply_block->set_sampled(true);

                }
                else {
                  reply_block->set_sampled(false);
                }
                reply_block->set_cached(false); 
                wait_accdata = true;
              }
              cache_table.insert(std::make_pair(blockId, true));
            }
          }
          // add block to task
          task->addInputBlock(blockId, block);

          // add block information to a table
          block_table.insert(std::make_pair(blockId,
                std::make_pair(reply_block->cached(), 
                  reply_block->sampled())));
        }
      }

      // 1.4 decide to reject the task if wait time is too long
#if 0
      int task_time = task_manager.lock()->estimateTime(task.get());
      int task_speedup = task->estimateSpeedup();

      if (task_time > 0 && task_speedup > 0) {

        // <bestcase wait time, worstcase wait time>
        std::pair<int,int> wait_time = task_manager.lock()->getWaitTime(task.get());

        VLOG(1) << "Wait time = (" << wait_time.first 
          << ", " << wait_time.second << ") us, task estimated time = "
          << task_time << " us";

        // use worst cast wait time
        if (wait_time.second > task_time*task_speedup) {
          throw AccReject("Wait time too long");
        }
      }
#endif

      // 1.5 send msg back to client
      reply_msg.set_type(ACCGRANT);
      try {
        send(reply_msg, sock);
      }
      catch (std::exception &e) {
        throw AccFailure(
            std::string("Error in sending ACCGRANT: ")+
            std::string(e.what()));
      }

      // 2. Handle ACCDATA
      if (wait_accdata) {

        TaskMsg data_msg;
        try {
          recv(data_msg, sock);
        }
        catch (std::exception &e) {
          throw AccFailure(
              std::string("Error in receiving ACCDATA ")+
              std::string(e.what()));
        }
        DLOG(INFO) << "Received ACCDATA";

        // Acquire data from Spark
        if (data_msg.type() != ACCDATA) {
          throw AccFailure("Expecting an ACCDATA");
        }

        // Loop through all the DataMsg
        for (int i=0; i<data_msg.data_size(); i++) {

          DataMsg recv_block = data_msg.data(i);

          // check message format
          if (!recv_block.has_partition_id() ||
              !recv_block.has_file_path()) 
          {
            throw AccFailure("Missing info in ACCDATA");
          }
          int64_t blockId = recv_block.partition_id();
          std::string path = recv_block.file_path();

          if (task->isInputReady(blockId)) {
            LOG(WARNING) << "Skipping ready block " << blockId;
            break;
          }
          VLOG(1) << "Start reading data for block " << blockId;

          // block_status: <cached, sampled>
          std::pair<bool, bool> block_status = block_table[blockId];

          DataBlock_ptr block;

          // 2.1 Getting data ready for the block 
          if (!block_status.first) { // block is not cached

            // check required fields
            if (!recv_block.has_file_path()) {
              throw AccFailure(std::string("Missing information for block " )+
                  std::to_string((long long)blockId));
            }
            std::string path = recv_block.file_path();

            // 2.1.1 Read data from filesystem 
            if (recv_block.has_num_elements() && 
                recv_block.num_elements() < 0) { 
              
              throw AccFailure("Reading filesystem is unsupported in this version");
            }
            // 2.1.2 Read input data block from memory mapped file
            else {  

              if (blockId >=0) {

                // if block is regular input then expect sizing information
                if (!recv_block.has_num_elements() ||
                    !recv_block.has_element_length() ||
                    !recv_block.has_element_size())
                {
                  throw AccFailure("Missing block size in ACCDATA");
                }

                int num_elements    = recv_block.num_elements();
                int element_length  = recv_block.element_length();
                int element_size    = recv_block.element_size();

                // check task config table to see if task is aligned
                int align_width = 0;
                if (!task->getConfig(i, "align_width").empty()) {
                  align_width = stoi(task->getConfig(i, "align_width"));
                }

                if ( cache_table.find(blockId) != cache_table.end() &&
                    !cache_table[blockId]) 
                {
                  DLOG(INFO) << "Skip cache for block " << blockId;

                  // the block should skip cache
                  block = platform->createBlock(
                      num_elements, element_length, element_size,
                      align_width);
                }
                else {
                  // the block needs to be created and add to cache
                  block_manager->getAlloc(
                      blockId, block,
                      num_elements, element_length, element_size, 
                      align_width);
                }
              }
              else { 
                // if the block is a broadcast then it already resides in scratch
                block = block_manager->get(blockId);
              }

              try {
                block->readFromMem(path);
              }
              catch (std::exception &e) {
                LOG(ERROR) << "readFromMem error: " << e.what();
                throw AccFailure(std::string("readFromMem error"));
              }

              // NOTE: only remove normal input file
              if (!deleteFile(path)) {
                LOG(WARNING) << "Did not remove file for block " << blockId;
              } 
            }
          }
          else { 
            // block is cached
            block = block_manager->get(blockId);
          }

          // 2.2 add block to Task input table
          if (block_status.second) { // block is sampled

            if (!recv_block.has_mask_path() || 
                !recv_block.has_num_elements())
            {
              throw AccFailure(std::string("Mask path is missing for block ")+
                  std::to_string((long long)blockId));
            }
            std::string mask_path = recv_block.mask_path();
            int         mask_size = recv_block.num_elements();

            // read mask from memory mapped file
            boost::iostreams::mapped_file_source fin;
            fin.open(mask_path, mask_size);

            if (fin.is_open()) {
              char* mask = (char*)fin.data();

              // overwrite the block handle to the sampled block
              block = block->sample(mask);

              VLOG(1) << "Finish sampling block " << blockId;
            }
            else {
              throw AccFailure(std::string("Cannot mask for block ")+
                  std::to_string((long long)blockId));
            }
          }
          try {
            // add missing block to Task input_table, block should be ready
            task->inputBlockReady(blockId, block);
          } catch (std::exception &e) {
            throw AccFailure(std::string("Cannot ready input block ")+
                std::to_string((long long)blockId)+(" because: ")+
                std::string(e.what()));
          }
          VLOG(1) << "Finish reading data for block " << blockId;
        }
      } // 2. Finish handling ACCDATA

      // wait on task ready
      while (!task->isReady()) {
        boost::this_thread::sleep_for(
            boost::chrono::microseconds(100)); 
      }

      VLOG(2) << "Task ready, enqueue to be executed";

      // check if task_manager is still valid
      if (!task_manager.lock()) {
        // accelerator already removed
        DLOG(WARNING) << "Accelerator queue for " << acc_id
                      << " is likely to be removed";
        throw AccReject("No matching accelerator"); 
      }
      // add task to application queue
      task_manager.lock()->enqueue(app_id, task.get());

      // wait on task finish
      while (
          task->status != Task::FINISHED && 
          task->status != Task::FAILED) 
      {
        boost::this_thread::sleep_for(
            boost::chrono::microseconds(100)); 
      }

      // 3. Handle ACCFINISH message and output data
      TaskMsg finish_msg;

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
          std::stringstream path_stream;

          path_stream << local_dir << "/"
                      << "output-"
                      << getTid() 
                      << std::setw(12) << std::setfill('0') << rand() 
                      << outId
                      << ".dat";
          std::string path = path_stream.str();
          try {
            // write the block to output shared memory
            block->writeToMem(path);
          } 
          catch (std::exception &e) {
            throw AccFailure(
                std::string("writeToMem error: ")+
                e.what()+
                std::string("; path=")+path);
          }
          // construct DataMsg
          // NOTE: not considering output data aligned allocation
          DataMsg *block_info = finish_msg.add_data();
          block_info->set_partition_id(outId);
          block_info->set_file_path(path); 
          block_info->set_num_elements(block->getNumItems());	
          block_info->set_element_length(block->getItemLength());	
          block_info->set_element_size(block->getItemSize());	

          outId ++;
        }
        finish_msg.set_type(ACCFINISH);
        try {
          send(finish_msg, sock);
          VLOG(1) << "Task finished, sent an ACCFINISH";
        } catch (std::exception &e) {
          LOG(ERROR) << "Cannot send ACCFINISH";
        }
      }
      else {
        throw AccFailure("Task failed");
      }
    }
    // 4. Handle APPTERM
    else if (task_msg.type() == ACCTERM) {

      if (!task_msg.has_app_id()) {
        throw AccFailure("Missing app_id in ACCTERM");
      }
      std::string app_id = task_msg.app_id();

      LOG(INFO) << "Recieved an ACCTERM message for " << app_id;

      // TODO: delete application queue for app_id

      TaskMsg finish_msg;
      for (int d=0; d<task_msg.data_size(); d++) {
        const DataMsg blockInfo = task_msg.data(d);
        int64_t blockId = blockInfo.partition_id();

        // deleting an existing blocks from all block manager
        platform_manager->removeShared(blockId);
      }
      finish_msg.set_type(ACCFINISH);

      send(finish_msg, sock);
    }
    // 5. Handling ACCREGISTER
    else if (task_msg.type() == ACCREGISTER) {
      handleAccRegister(task_msg);

      // send ACCFINISH
      TaskMsg finish_msg;
      finish_msg.set_type(ACCFINISH);

      try {
        send(finish_msg, sock);
        VLOG(1) << "Sent an ACCFINISH regarding ACCREGISTER";
      } catch (std::exception &e) {
        LOG(WARNING) << "Cannot send ACCFINISH";
      }
    }
    else if (task_msg.type() == ACCDELETE) {
      handleAccDelete(task_msg);

      // send ACCFINISH
      TaskMsg finish_msg;
      finish_msg.set_type(ACCFINISH);

      try {
        send(finish_msg, sock);
        VLOG(1) << "Sent an ACCFINISH regarding ACCDELETE";
      } catch (std::exception &e) {
        LOG(WARNING) << "Cannot send ACCFINISH";
      }
    }
    else {
      char msg[500];
      sprintf(msg, "Unknown message type: %d, discarding message\n",
          task_msg.type());
      throw (AccFailure(msg));
    }
  }
  catch (AccReject &e)  {

    TaskMsg reply_msg;

    reply_msg.set_type(ACCREJECT);
    reply_msg.set_msg(e.what());

    LOG(ERROR) << "Send ACCREJECT because: " << e.what();
    try {
      send(reply_msg, sock);
    } catch (std::exception &e) {
      return;
    }
  }
  catch (AccFailure &e)  {

    TaskMsg reply_msg;

    reply_msg.set_type(ACCFAILURE);
    reply_msg.set_msg(e.what());

    LOG(ERROR) << "Send ACCFAILURE because: " << e.what();

    try {
      send(reply_msg, sock);
    } catch (std::exception &e) {
      return;
    }
  }
  catch (std::runtime_error &e)  {

    TaskMsg reply_msg;

    reply_msg.set_type(ACCFAILURE);

    LOG(ERROR) << "Send ACCFAILURE because: " << e.what();

    try {
      send(reply_msg, sock);
    } catch (std::exception &e) {
      return;
    }
  }
  catch (std::exception &e) {
    LOG(ERROR) << "Unexpected exception: " << e.what();
  }
}
void AppCommManager::handleAccRegister(TaskMsg &msg) {

  if (!msg.has_acc()) {
    throw AccReject("missing AccMsg");
  }

  // unpack message
  AccMsg acc_msg = msg.acc();
  std::string acc_id = acc_msg.acc_id();
  std::string platform_id = acc_msg.platform_id();

  // check if platform_id and acc_id is valid
  if (platform_manager->accExists(acc_id)) 
  {
    LOG(WARNING) << "Cannot register accelerator ["
      << acc_id << "] on platform [" << platform_id
      << "] because: accelerator exists";

    throw (AccReject("Accelerator exists"));
  }
  if (!platform_manager->platformExists(platform_id)) 
  {
    LOG(WARNING) << "Cannot register accelerator ["
      << acc_id << "] on platform [" << platform_id
      << "] because: platform does not exist";

    throw (AccReject("Platform does not exist"));
  }

  // create AccWorker
  AccWorker acc_conf;
  acc_conf.set_id(acc_id);

  // setup parameters and transfer files
  //std::string root_dir = local_dir + "/" + acc_id + "/"; 

  //std::string path = root_dir+std::string("task.so");

  //path = saveFile(path, acc_msg.task_impl());
  acc_conf.set_path(acc_msg.task_impl());

  // TODO: here is an issue when the file size of very large,
  // in the received binary string there are usally large 
  // amount of data being wrong
  //VLOG(1) << "Saved acc task in " << path;

  for (int i=0; i<acc_msg.param_size(); i++) {
    AccWorker::KeyValue* new_param = acc_conf.add_param();

    std::string key = acc_msg.param(i).key();
    new_param->set_key(key);

    // if key specifies a file path
    //if (key.length() > 5 && 
    //    key.substr(key.length()-5, 5) == "_path")
    //{
    //  std::string value_path = saveFile(
    //      root_dir+key, acc_msg.param(i).value());

    //  VLOG(1) << "Saved file for '" << key << "'"
    //    << " in " << value_path;

    //  new_param->set_value(value_path);
    //}
    //else 
    {
      new_param->set_value(acc_msg.param(i).value());
    }
  }
  // setup Acc Queue
  try {
    platform_manager->registerAcc(platform_id, acc_conf);
  }
  catch (std::runtime_error &e) {
    throw AccFailure(e.what());
  }
}

void AppCommManager::handleAccDelete(TaskMsg &msg) {
  if (!msg.has_acc()) {
    throw AccReject("missing AccMsg");
  }
  // unpack message
  AccMsg acc_msg = msg.acc();
  std::string acc_id = acc_msg.acc_id();
  std::string platform_id = acc_msg.platform_id();
  
  if (!platform_manager->accExists(acc_id)) {
    VLOG(1) << "Accelerator " << acc_id << " does not exist, "
            << "assumes deletion successful.";
    return;
  }
  try {
    platform_manager->removeAcc("", acc_id, platform_id);
  }
  catch (std::exception &e) {
    throw AccFailure(e.what());
  }
  //std::string root_dir = local_dir + "/" + acc_id; 
  //if (deleteFile(root_dir)) {
  //  DLOG(INFO) << "Deleted accelerator from " << root_dir;
  //} else {
  //  DLOG(ERROR) << "Failed to delete accelerator from " << root_dir;
  //}
}
} // namespace blaze

