#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <fcntl.h>   
#include <sys/mman.h>
#include <sys/stat.h>

#include <iostream>
#include <stdexcept>
#include <cstdint>

#include "CommManager.h"

#define MAX_MSGSIZE 4096
#define LOG_HEADER  std::string("CommManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")

namespace blaze {

// receive one message, bytesize first
void CommManager::recv(
    TaskMsg &task_msg, 
    socket_ptr socket) 
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
void CommManager::send(
    TaskMsg &task_msg, 
    socket_ptr socket) 
{
  int msg_size = task_msg.ByteSize();

  //NOTE: why doesn't this work: socket_stream << msg_size;
  socket->send(buffer(reinterpret_cast<char*>(&msg_size), sizeof(int)),0);

  char* msg_data = new char[msg_size];

  task_msg.SerializeToArray(msg_data, msg_size);

  socket->send(buffer(msg_data, msg_size),0);
}

void CommManager::addTask(std::string id) {
  // guarantee exclusive access
  boost::lock_guard<CommManager> guard(*this);
  if (num_tasks.find(id) == num_tasks.end()) {
    num_tasks.insert(std::make_pair(id, 0));
  }
  else {
    num_tasks[id] += 1;
  }
}

void CommManager::removeTask(std::string id) {
  // guarantee exclusive access
  boost::lock_guard<CommManager> guard(*this);
  if (num_tasks.find(id) != num_tasks.end()) {
    num_tasks[id] -= 1;
  }
}

void CommManager::process(socket_ptr sock) {

  // turn off Nagle Algorithm to improve latency
  sock->set_option(ip::tcp::no_delay(true));
  
  // log info
  std::string msg = 
    LOG_HEADER + 
    std::string("Start processing a new connection.");
  logger->logInfo(msg);

  // recording whether the task is active or not
  bool do_task = false;
  std::string task_id;
  srand(time(NULL));

  try {
    // 1. Handle ACCREQUEST
    TaskMsg task_msg;
    TaskMsg reply_msg;

    // a table containing information of each input block
    // - partition_id: cached, sampled
    std::map<int64_t, std::pair<bool, bool> > block_table;

    try {
      recv(task_msg, sock);
    }
    catch (std::runtime_error &e){
      throw AccFailure("Error in receiving ACCREQUEST");
    }
    if (task_msg.type() == ACCREQUEST) {

      logger->logInfo(
          LOG_HEADER + 
          std::string("Received an ACCREQUEST message."));

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
        logger->logErr(LOG_HEADER+
            std::string("Requested acc ")+
            task_msg.acc_id()+
            std::string(" does not exist"));

        throw AccReject("No matching accelerator, rejecting request"); 
      }
      else { 
        // Calculating scheduling decisions
        // TODO: use a separate class for this part
        //if (rand() % 2 == 0) {
        //  throw AccReject("Rejecting ACC to balance workload");
        //}
      }

      // keep track of a new active task
      addTask(task_msg.acc_id());
      do_task = true;
      task_id = task_msg.acc_id();

      // 1.2 get correponding block manager based on platform of queried Task
      BlockManager* block_manager = platform_manager->
        getBlockManager(task_msg.acc_id());

      // create a new task, which will be automatically added to the task queue
      Task* task = task_manager->create();

      bool wait_accdata = false;

      // 1.3 iterate through each input block
      for (int i = 0; i < task_msg.data_size(); i++) {

        DataMsg recv_block = task_msg.data(i);

        // 1.3.1 if input is a scalar
        if (recv_block.has_bval()) { 

          int64_t bval = recv_block.bval();

          // add the scalar as a new block
          DataBlock_ptr block(new DataBlock(1, 8));
          block->writeData((void*)&bval, 8);

          /* skip cache and directly add it to task
           * NOTE: use naive block id here, since the id is only
           * used to differentiate the blocks in a single task
           */
          task->addInputBlock(i, block);
        }
        // 1.3.2 if this is not a scalar, then its an array
        else {

          int64_t blockId = recv_block.partition_id();

          // reply entry in ACCGRANT
          DataMsg *reply_block = reply_msg.add_data();
          reply_block->set_partition_id(blockId);

          // reference block to add to the task's input table
          DataBlock_ptr block;

          // 1.3.2.1 if the input is a broadcast block
          if (blockId < 0) {

            if (block_manager->contains(blockId)) {

              block = block_manager->get(blockId);
              reply_block->set_cached(true); 
            }
            else {

              /* create a new empty block and add it to broadcast cache
               * NOTE: at this point multiple threads may try 
               * to create the same block, only one can be successful
               */
              bool created = block_manager->create(blockId, block);

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

      // 1.4 send msg back to client
      reply_msg.set_type(ACCGRANT);
      send(reply_msg, sock);
      logger->logInfo(
          LOG_HEADER + 
          std::string("Replied with an ACCGRANT message."));

      // 2. Handle ACCDATA
      if (wait_accdata) {

        TaskMsg data_msg;
        try {
          recv(data_msg, sock);
        }
        catch (std::runtime_error &e) {
          throw AccFailure("Error in receiving ACCDATA");
        }
        logger->logInfo(LOG_HEADER + 
            std::string("Received an ACCDATA message."));

        // Acquire data from Spark
        if (data_msg.type() != ACCDATA) {
          throw AccFailure("Expecting an ACCDATA");
        }

        // Loop through all the DataMsg
        for (int i=0; i<data_msg.data_size(); i++) {

          DataMsg recv_block = data_msg.data(i);
          int64_t blockId = recv_block.partition_id();

          // input block of the task
          //DataBlock_ptr input_block = task->getInputBlock(partition_id);

          //if (input_block == NULL_DATA_BLOCK) {
          //  throw AccFailure(std::string("Did not find block: ")+
          //      std::to_string((long long)blockId));
          //}
          if (task->getInputBlock(blockId) &&
              task->getInputBlock(blockId)->isReady()) 
          {
            logger->logInfo(LOG_HEADER+
                "Skipping ready block "+
                std::to_string((long long)blockId));
            break;
          }
          logger->logInfo(LOG_HEADER+
              "Start reading data for block "+
              std::to_string((long long)blockId));

          DataBlock_ptr block;
          std::pair<bool, bool> block_status = block_table[blockId];

          // 2.1 Getting data ready for the block 
          if (!block_status.first) { // block is not cached

            if (blockId < 0) {
              // broadcast block is already added to the block_manager
              block = block_manager->get(blockId);
            }
            else {
              // create an empty block and add it to cache later
              block = block_manager->create();
            }

            // check required fields
            if (!recv_block.has_length() ||
                !recv_block.has_size() ||
                !recv_block.has_path())
            {
              throw AccFailure(std::string("Missing information for block " )+
                  std::to_string((long long)blockId));
            }

            int         length = recv_block.length();
            std::string path   = recv_block.path();

            // 2.1.1 Read data from filesystem
            if (length == -1) { 

              if (!recv_block.has_size() ||
                  !recv_block.has_offset())
              {
                throw AccFailure(std::string(
                      "Missing information to read from file for block ")+ 
                    std::to_string((long long)blockId));
              }
              int64_t size   = recv_block.size();	
              int64_t offset = recv_block.offset();

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
              size_t total_bytes = 0;

              // split the file by newline
              std::istringstream sstream(line_buffer);
              std::string line;

              size_t num_bytes = 0;      
              size_t num_elements = 0;      

              while(std::getline(sstream, line)) {

                char* data;
                try {
                  data = task->readLine(line, num_elements, num_bytes);
                } catch (std::runtime_error &e) {
                  logger->logErr(LOG_HEADER+
                      std::string("problem reading a line"));
                }
                if (num_bytes > 0) {
                  data_buf.push_back(std::make_pair(num_bytes, data));
                  total_bytes += num_bytes;
                }
              }
              // writing data to the corresponding block
              if (total_bytes > 0) {

                // lock block for exclusive access
                boost::lock_guard<DataBlock> guard(*block);

                // copy data to block
                block->alloc(total_bytes);

                size_t offset = 0;
                for (int i=0; i<data_buf.size(); i++) {
                  size_t bytes = data_buf[i].first;
                  char* data   = data_buf[i].second;

                  block->writeData((void*)data, bytes, offset);
                  offset += bytes;

                  delete data;
                }

                // the number of items is equal to the number of lines
                block->setNumItems(data_buf.size());

                // the total data length is num_elements * num_lines
                block->setLength(num_elements*data_buf.size());
              }

            }
            // 2.1.2 Read input data block from memory mapped file
            else {  

              if (!recv_block.has_size() ||
                  (recv_block.partition_id()>=0 && 
                   !recv_block.has_num_items()))
              {
                throw AccFailure(std::string(
                      "Missing information to read from memory for block ")+ 
                    std::to_string((long long)blockId));
              }
              int64_t size      = recv_block.size();	
              // TODO: this should not be empty fix AccRDD later
              int     num_items = recv_block.has_num_items() ?
                recv_block.num_items() : 1;

              // lock block for exclusive access
              boost::lock_guard<DataBlock> guard(*block);

              // allocate memory for block
              // check config_table of Task to see if data needs to be aligned
              if (!task->getConfig(blockId, "align_width").empty()) {
                int align_width = stoi(task->getConfig(
                      blockId, "align_width"));

                block->alloc(
                    num_items, 
                    length/num_items,
                    size/length,
                    align_width);
              }
              else {
                block->setLength(length);
                block->setNumItems(num_items);

                block->alloc(size);
              }

              block->readFromMem(path);

              // delete memory map file after read
              boost::filesystem::wpath file(path);
              if (boost::filesystem::exists(file)) {
                boost::filesystem::remove(file);
              }
            }
            // add the block to cache if the block is not a broadcast block
            // broadcast block is already added to the BlockManager
            if (blockId >= 0) {
              block_manager->add(blockId, block); 
            }
          }
          else { // block is already cached in the BlockManager
            block = block_manager->get(blockId);
          }

          // 2.2 add block to Task input table
          if (block_status.second) { // block is sampled

            if (!recv_block.has_mask_path() || 
                !recv_block.has_num_items())
            {
              throw AccFailure(std::string("Mask path is missing for block ")+
                  std::to_string((long long)blockId));
            }
            std::string mask_path = recv_block.mask_path();
            int         data_size = recv_block.num_items();

            // read mask from memory mapped file
            boost::iostreams::mapped_file_source fin;
            fin.open(mask_path, data_size);

            if (fin.is_open()) {
              char* mask = (char*)fin.data();

              // overwrite the block handle to the sampled block
              block = block->sample(mask);

              logger->logInfo(LOG_HEADER+
                  "Finish sampling block "+
                  std::to_string((long long)blockId));
            }
            else {
              throw AccFailure(std::string("Cannot mask for block ")+
                  std::to_string((long long)blockId));
            }
          }
          try {
            // add missing block to Task input_table, block should be ready
            task->inputBlockReady(blockId, block);
          } catch (std::runtime_error &e) {
            throw AccFailure(std::string("Cannot ready input block ")+
                std::to_string((long long)blockId)+(" because: ")+
                std::string(e.what()));
          }
          logger->logInfo(LOG_HEADER+
              "finish reading data for block "+
              std::to_string((long long)blockId));
        }
      } // 2. Finish handling ACCDATA

      // wait on task finish
      while (
          task->status != Task::FINISHED && 
          task->status != Task::FAILED) 
      {
        boost::this_thread::sleep_for(
            boost::chrono::microseconds(10)); 
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
            logger->getTid() + 
            std::to_string((long long)outId);

          try {
            // write the block to output shared memory
            block->writeToMem(path);
          } 
          catch ( std::runtime_error &e ) {
            throw AccFailure(
                std::string("writeToMem error: ")+
                e.what());
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
        finish_msg.set_type(ACCFINISH);
        send(finish_msg, sock);

        logger->logInfo(LOG_HEADER + 
            std::string("Task finished, sent an ACCFINISH."));
      }
      else {
        throw AccFailure("Task failed");
      }
    }
    // 4. Handle ACCBROADCAST
    else if (task_msg.type() == ACCBROADCAST) {

      // NOTE: 
      // In this implementation, ACCBROADCAST is used only for 
      // removing the broadcast block for an application

      logger->logInfo(LOG_HEADER + 
          std::string("Recieved an ACCBROADCAST message")) ; 

      TaskMsg finish_msg;
      for (int d=0; d< task_msg.data_size(); d++) {
        const DataMsg blockInfo = task_msg.data(d);
        int64_t blockId = blockInfo.partition_id();

        // deleting an existing blocks from all block manager
        platform_manager->removeShared(blockId);
      }
      finish_msg.set_type(ACCFINISH);
      logger->logInfo(LOG_HEADER+
          "Replied an ACCFINISH message regarding the broadcast.");

      send(finish_msg, sock);
    }
    else {
      throw (AccFailure("Unknown message type, discarding message."));
    }
  }
  catch (AccReject &e)  {

    TaskMsg reply_msg;

    reply_msg.set_type(ACCREJECT);

    logger->logInfo(
        LOG_HEADER+
        std::string("Send ACCREJECT because: ")+
        e.what());

    send(reply_msg, sock);
  }
  catch (AccFailure &e)  {

    TaskMsg reply_msg;

    reply_msg.set_type(ACCFAILURE);

    logger->logInfo(LOG_HEADER+
        std::string("Send ACCFAILURE because: ")+
        e.what());

    send(reply_msg, sock);
  }
  catch (std::runtime_error &e)  {

    TaskMsg reply_msg;

    reply_msg.set_type(ACCFAILURE);

    logger->logInfo(LOG_HEADER+
        std::string("Send ACCFAILURE because: ")+
        e.what());

    send(reply_msg, sock);
  }
  if (do_task) {
    removeTask(task_id);
  }
  logger->logInfo(LOG_HEADER+"thread exiting.");
}

void CommManager::listen() {

  try {
    io_service ios;

    ip::tcp::endpoint endpoint(
        ip::address::from_string(ip_address),
        srv_port);

    ip::tcp::acceptor acceptor(ios, endpoint);

    logger->logInfo(LOG_HEADER + 
        std::string("Listening for new connections at ")+
        ip_address + std::string(":") + std::to_string((long long)srv_port));

    // TODO: join all thread after termination
    while(1) {

      // create socket for connection
      socket_ptr sock(new ip::tcp::socket(ios));

      // accept incoming connection
      acceptor.accept(*sock);

      //acceptor.accept(*socket_stream.rdbuf());
      boost::thread t(boost::bind(&CommManager::process, this, sock));
    }
  }
  catch (std::exception &e) {
    // do not throw exception, just end current thread
    logger->logErr(LOG_HEADER+e.what());
  }
}
} // namespace blaze
