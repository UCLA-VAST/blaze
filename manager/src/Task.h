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

/**
 * Task is the base clase of an accelerator task
 * will be extended by user 
 */
class Task {

friend class TaskManager;
friend class CommManager;

public:
  Task(int _num_input): 
    status(NOTREADY), 
    num_input(_num_input),
    num_ready(0)
  {;}

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
      
      int length = num_items*item_length;

      // create platform-specific block object
      DataBlock_ptr block = platform->createBlock(length, length*data_width);
     
      block->setNumItems(num_items);

      output_blocks.push_back(block);

      return block->getData();
    }
  }

  int getInputLength(int idx) { 
    if (idx < input_blocks.size()) {
      return input_blocks[idx]->getLength(); 
    }
    else {
      throw std::runtime_error("getInputLength out of bound idx");
    }
  }

  int getInputNumItems(int idx) { 
    if (idx < input_blocks.size()) {
      return input_blocks[idx]->getNumItems() ; 
    }
    else {
      throw std::runtime_error("getInputNumItems out of bound idx");
    }
  }

  char* getInput(int idx) {
    if (idx < input_blocks.size()) {
      return input_blocks[idx]->getData();      
    }
    else {
      throw std::runtime_error("getInput out of bound idx");
    }
  }

private:

  void addInputBlock(int64_t partition_id, DataBlock_ptr block);

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

  // input and output data block
  std::vector<DataBlock_ptr> input_blocks;
  std::vector<DataBlock_ptr> output_blocks;

  std::map<int64_t, DataBlock_ptr> input_table;
};

typedef boost::shared_ptr<Task> Task_ptr;
}
#endif
