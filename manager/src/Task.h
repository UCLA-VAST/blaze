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
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

#include "proto/task.pb.h"

#include "Block.h"
#include "Platform.h"

namespace blaze {

// forward declaration of 
class TaskManager;
class CommManager;
template <typename U, typename T> class BlazeTest;

/**
 * Task is the base clase of an accelerator task
 * will be extended by user 
 */
class Task {

friend class TaskManager;
friend class CommManager;
template <typename U, typename T> 
friend class BlazeTest;

public:
  Task(int _num_input): 
    status(NOTREADY), 
    num_input(_num_input),
    num_ready(0)
  {; }

  void setPlatform(Platform *_platform) {
    platform = _platform;  
  }

  // main function to be overwritten by accelerator implementations
  virtual void compute() {;}

  // wrapper around compute(), added indicator for task status
  void execute() {
    try {
      compute();
      status = FINISHED;
    } catch (std::runtime_error &e) {
      status = FAILED; 
      throw e;
    }
  }
  
  std::string getConfig(int64_t id, std::string key) {

    // search input_blocks for matching partition
    int idx;
    for (idx=0; idx<input_blocks.size(); idx++) {
      if (input_blocks[idx] == id) {
        break;
      } 
    }
    // matching block is not found
    if (idx == input_blocks.size()) {
      return std::string(); 
    }
    if (config_table[idx].find(key) != config_table[idx].end()) {
      return config_table[idx][key];
    } else {
      return std::string();
    }
  }

protected:

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

  TaskEnv* getEnv() {
    return platform->getEnv();  
  }

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
      // if output does not exist, create one
      DataBlock_ptr block = platform->createBlock(num_items, 
          item_length, item_length*data_width);

      output_blocks.push_back(block);

      return block->getData();
    }
  }

  int getInputLength(int idx) { 
    if (idx < input_blocks.size()) {
      return input_table[input_blocks[idx]]->getLength(); 
    }
    else {
      throw std::runtime_error("getInputLength out of bound idx");
    }
  }

  int getInputNumItems(int idx) { 
    if (idx < input_blocks.size()) {
      return input_table[input_blocks[idx]]->getNumItems() ; 
    }
    else {
      throw std::runtime_error("getInputNumItems out of bound idx");
    }
  }

  char* getInput(int idx) {
    
    if (idx < input_blocks.size()) {
      return input_table[input_blocks[idx]]->getData();      
    }
    else {
      throw std::runtime_error("getInput out of bound idx");
    }
  }

  // add a configuration for a dedicated block 
  void addConfig(int idx, std::string key, std::string val) {

    config_table[idx][key] = val;
  }

private:

  void addInputBlock(int64_t partition_id, DataBlock_ptr block);

  void inputBlockReady(int64_t partition_id, DataBlock_ptr block);

  DataBlock_ptr getInputBlock(int64_t block_id);

  // push one output block to consumer
  // return true if there are more blocks to output
  bool getOutputBlock(DataBlock_ptr &block);
   
  DataBlock_ptr onDataReady(const DataMsg &blockInfo);

  bool isReady();

  enum {
    NOTREADY,
    READY,
    FINISHED,
    FAILED,
    COMMITTED
  } status;

  // pointer to the platform
  Platform *platform;

  // number of total input blocks
  int num_input;

  // number of input blocks that has data initialized
  int num_ready;

  // a mapping between partition_id to the input blocks
  std::map<int64_t, DataBlock_ptr> input_table;

  // a list of input blocks to its partition_id
  std::vector<int64_t> input_blocks;

  // list of output blocks
  std::vector<DataBlock_ptr> output_blocks;

  // a table that maps block index to configurations
  std::map<int, std::map<std::string, std::string> > config_table;
};

typedef boost::shared_ptr<Task> Task_ptr;
}
#endif
