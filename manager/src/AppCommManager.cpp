#include <stdio.h>
#include <time.h>
#include <fcntl.h>   
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <fstream>
#include <stdexcept>
#include <cstdint>

#include <boost/iostreams/device/mapped_file.hpp>

#include <glog/logging.h>

#include "proto/task.pb.h"
#include "CommManager.h"

namespace blaze {

void AppCommManager::process(socket_ptr sock) {

  // turn off Nagle Algorithm to improve latency
  sock->set_option(ip::tcp::no_delay(true));

  // set socket buffer size to be 4MB
  socket_base::receive_buffer_size option(4*1024*1024);
  sock->set_option(option); 
  
  srand(time(NULL));

  try {
    // 1. Handle ACCREQUEST
    TaskMsg task_msg;
    TaskMsg reply_msg;

    std::string app_id;
    std::string acc_id;

    // a table containing information of each input block
    // - partition_id: cached, sampled
    std::map<int64_t, std::pair<bool, bool> > block_table;

    try {
      recv(task_msg, sock);
    }
    catch (std::exception &e){
      throw AccFailure(
          std::string("Error in receiving ACCREQUEST: ")+
          std::string(e.what()));
    }
    if (task_msg.type() == ACCREQUEST) {

      if (!task_msg.has_acc_id() || !task_msg.has_app_id()) {
        throw AccReject("Missing acc_id or app_id");
      }
      acc_id = task_msg.acc_id();
      app_id = task_msg.app_id();

      LOG(INFO) << "Received an ACCREQUEST for " << acc_id
        << "from app " << app_id;

      /* Receive acc_id to identify the corresponding TaskManager, 
       * with which create a new Task. 
       * Check the list of DataMsg to register input data blocks in the 
       * task's input table, in order to get the correct order
       */

      // 1.1 query the queue manager to find matching acc
      TaskManager_ptr task_manager = 
        platform_manager->getTaskManager(task_msg.acc_id());

      if (task_manager == NULL_TASK_MANAGER) { 
        // if there is no matching acc
        throw AccReject("No matching accelerator"); 
      }

      // 1.2 get correponding block manager based on platform of queried Task
      BlockManager* block_manager = platform_manager->
        getBlockManager(task_msg.acc_id());

      // create a new task, and make scheduling decision
      Task_ptr task = task_manager->create();
      
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
      int task_time = task_manager->estimateTime(task.get());
      int task_speedup = task->estimateSpeedup();

      if (task_time > 0 && task_speedup > 0) {

        // <bestcase wait time, worstcase wait time>
        std::pair<int,int> wait_time = task_manager->getWaitTime(task.get());

        LOG(INFO) << "Wait time = (" << wait_time.first 
          << ", " << wait_time.second << ") us, task estimated time = "
          << task_time << " us";

        // use worst cast wait time
        if (wait_time.second > task_time*task_speedup) {
          throw AccReject("Wait time too long");
        }
      }

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

          if (task->getInputBlock(blockId) &&
              task->getInputBlock(blockId)->isReady()) 
          {
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
              
              LOG(WARNING) << "Reading file is unstable!";

              if (!recv_block.has_file_size() ||
                  !recv_block.has_file_offset())
              {
                throw AccFailure(std::string(
                      "Missing information to read from file for block ")+ 
                    std::to_string((long long)blockId));
              }
              int64_t size   = recv_block.file_size();	
              int64_t offset = recv_block.file_offset();

              //std::vector<std::string> lines;
              char* buffer = new char[size];

              if (path.compare(0, 7, "hdfs://") == 0) { // read from HDFS
#ifdef USE_HDFS
                if (!getenv("HDFS_NAMENODE") ||
                    !getenv("HDFS_PORT"))
                {
                  throw std::runtime_error(
                      "no HDFS_NAMENODE or HDFS_PORT defined");
                }

                std::string hdfs_name_node = getenv("HDFS_NAMENODE");
                uint16_t hdfs_port = 
                  boost::lexical_cast<uint16_t>(getenv("HDFS_PORT"));

                hdfsFS fs = hdfsConnect(hdfs_name_node.c_str(), hdfs_port);

                if (!fs) {
                  throw std::runtime_error("Cannot connect to HDFS");
                }

                hdfsFile fin = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0); 

                if (!fin) {
                  throw std::runtime_error("Cannot find file in HDFS");
                }

                int err = hdfsSeek(fs, fin, offset);
                if (err != 0) {
                  throw std::runtime_error(
                      "Cannot read HDFS from the specific position");
                }

                int64_t bytes_read = hdfsRead(
                    fs, fin, (void*)buffer, size);

                if (bytes_read != size) {
                  throw std::runtime_error("HDFS read error");
                }

                hdfsCloseFile(fs, fin);
#else 
                throw std::runtime_error("HDFS file is not supported");
#endif
              }
              else { // read from normal file
                std::ifstream fin(path, std::ifstream::binary); 

                if (!fin) {
                  throw std::runtime_error("Cannot find file");
                }

                // TODO: error handling
                fin.seekg(offset);
                fin.read(buffer, size);
                fin.close();
              }

              std::string line_buffer(buffer);
              delete buffer;

              // buffer for all data
              std::vector<std::pair<size_t, char*> > data_buf;
              size_t total_size = 0;
              size_t item_length = 0;      
              size_t item_size = 0;      

              // split the file by newline
              std::istringstream sstream(line_buffer);
              std::string line;

              while(std::getline(sstream, line)) {
                char* data;
                try {
                  data = task->readLine(line, item_length, item_size);
                } catch (std::runtime_error &e) {
                  LOG(ERROR) << "Fail to read line";
                }
                if (item_size > 0) {
                  data_buf.push_back(std::make_pair(item_size, data));
                  total_size += item_size;
                }
              }
              if (total_size <= 0) {
                LOG(ERROR) << "Did not read any data for block " << blockId;
              }
              // writing data to the corresponding block
              int align_width = 0;
              if (!task->getConfig(i, "align_width").empty()) {
                 align_width = stoi(task->getConfig(i, "align_width"));
              }

              block_manager->getAlloc(
                  blockId, block,
                  data_buf.size(), item_length, item_size, 
                  align_width);

              int item_offset = 0;

              // lock block for exclusive access during write
              boost::lock_guard<DataBlock> guard(*block);
              for (int i=0; i<data_buf.size(); i++) {
                size_t bytes = data_buf[i].first;
                char* data   = data_buf[i].second;

                block->writeData((void*)data, bytes, item_offset);
                item_offset += block->getItemSize();

                delete data;
              }
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

                // the block needs to be created and add to cache
                block_manager->getAlloc(
                    blockId, block,
                    num_elements, element_length, element_size, 
                    align_width);
              }
              else { 
                // if the block is a broadcast then it already resides in scratch
                block = block_manager->get(blockId);
              }

              try {
                block->readFromMem(path);
              }
              catch (std::exception &e) {
                throw AccFailure(std::string("readFromMem error: ")+
                    e.what());
              }

              try {
                // delete memory map file after read
                boost::filesystem::wpath file(path);
                if (boost::filesystem::exists(file)) {
                  boost::filesystem::remove(file);
                }
              } catch (std::exception &e) {
                LOG(WARNING) << "Cannot delete memory mapped file after read";
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

      // add task to application queue
      task_manager->enqueue(app_id, task.get());

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
          std::string path = 
            "/tmp/" + 
            boost::lexical_cast<std::string>(boost::this_thread::get_id())+
            std::to_string((long long)outId);

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
        } catch (std::exception &e) {
          throw AccFailure("Cannot send ACCFINISH");
        }

        VLOG(1) << "Task finished, sent an ACCFINISH";
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
} // namespace blaze

