#ifndef TASK_H
#define TASK_H

#include <stdio.h>
#include <map>
#include <vector>
#include <cstdlib>
#include <fstream>

#ifdef USEHDFS
#include "hdfs.h"
#endif

#include <boost/lexical_cast.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

#include "Block.h"
#include "OpenCLBlock.h"
#include "TaskEnv.h"
#include "OpenCLEnv.h"

namespace acc_runtime {
/**
 * Task is the base clase of an accelerator task
 * will be extended by user 
 */
class Task {

public:
  // TODO: this should not be public
  enum {
    NOTREADY,
    READY,
    FINISHED,
    FAILED,
    COMMITTED
  } status;

  Task(TaskEnv *_env, int _num_input): 
    env(_env),
    status(NOTREADY), 
    num_input(_num_input),
    num_ready(0)
  {;}

  // main function to be overwritten by accelerator implementations
  virtual void compute() {;}

  // wrapper around compute(), added indicator for task status
  void execute() {
    try {
      compute();
    } catch (std::runtime_error &e) {
      status = FAILED; 
      printf("Task failed: %s\n", e.what());
      return;
    }
    status = FINISHED;
  }

  void addInputBlock(int64_t partition_id, DataBlock_ptr block) {

    input_blocks.push_back(block);

    // add the same block to a map table to provide fast access
    input_table.insert(std::make_pair(partition_id, block));

    // automatically trace all the blocks,
    // if all blocks are initialized with data, 
    // set the task status to READY
    if (block->isReady()) {
      num_ready ++;
      if (num_ready == num_input) {
        status = READY;
      }
    }
  }

  // push one output block to consumer
  DataBlock_ptr getOutputBlock() {
    
    if (!output_blocks.empty()) {

      DataBlock_ptr block = output_blocks.back();

      // assuming the blocks are controlled by consumer afterwards
      output_blocks.pop_back();

      // no more output blocks means all data are consumed
      if (output_blocks.empty()) {
        status = COMMITTED;
      }
      return block;
    }
    else {
      return NULL_DATA_BLOCK;
    }
  }

  // read one line from file and write to an array
  // and return the size of bytes put to a buffer
  virtual char* readLine(
      std::string line, 
      size_t &num_elements,
      size_t &num_bytes) 
  {
    num_bytes = 0; 
    num_elements = 0;
    return NULL;
  }

  DataBlock_ptr onDataReady(
      int64_t partition_id, 
      int length, 
      int num_items, 
      int64_t size, 
      int64_t offset,
      std::string path) 
  {
    DataBlock_ptr block = input_table[partition_id];

    if (!block->isReady()) {

      if (length == -1) { // read from file

        //std::vector<std::string> lines;
        char* buffer = new char[size];

        if (path.compare(0, 7, "hdfs://") == 0) {
          // read from HDFS
          
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
        else {
          // read from normal file
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
            data = readLine(line, num_elements, num_bytes);
          } catch (std::runtime_error &e) {
            throw e; 
          }

          if (num_bytes > 0) {
            data_buf.push_back(std::make_pair(num_bytes, data));

            total_bytes += num_bytes;

          }
        }

        if (total_bytes > 0) {
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
      else {  // read from memory mapped file

        block->setLength(length);
        block->setNumItems(num_items);

        // allocate memory for block
        block->alloc(size);

        block->readFromMem(path);
      }

      num_ready++;

      if (num_ready == num_input) {
        status = READY;
      }
    }

    return block;
  }

protected:

  char* getOutput(
      int idx, 
      int item_length, 
      int num_items,
      int data_width) 
  {

    if (idx < output_blocks.size()) {
      // if output already exists, return the pointer 
      // to the existing block
      return output_blocks[idx]->getData();
    }
    else {
      int length = num_items*item_length;

      // if output does not exist, create one

      DataBlock_ptr block;
      
      switch (env->getType()) {
        case AccType::CPU: 
        {
          DataBlock_ptr bp(new DataBlock(length, length*data_width));
          block = bp;
          break;
        }   
        case AccType::OpenCL:
        {
          DataBlock_ptr bp(new OpenCLBlock(
              reinterpret_cast<OpenCLEnv*>(env), 
              length, length*data_width));
          block = bp;
          break;
        }
        default: {
          DataBlock_ptr bp(new DataBlock(length, length*data_width));
          block = bp;
        }
      }
      block->setNumItems(num_items);

      output_blocks.push_back(block);

      return block->getData();
    }
  }

  int getInputLength(int idx) { 
    return input_blocks[idx]->getLength(); 
  }

  int getInputNumItems(int idx) { 
    return input_blocks[idx]->getNumItems() ; 
  }

  char* getInput(int idx) {
    return input_blocks[idx]->getData();      
  }

  // pointer to task environment
  // accessable by the extended tasks
  TaskEnv *env;

private:
  int num_input;

  // number of input blocks that has data initialized
  int num_ready;

  // remove the access to ACC programmer
  std::vector<DataBlock_ptr> input_blocks;
  std::vector<DataBlock_ptr> output_blocks;

  std::map<int64_t, DataBlock_ptr> input_table;
};

}
#endif
