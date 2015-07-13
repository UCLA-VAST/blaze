#ifndef TASK_H
#define TASK_H

#include <map>
#include <vector>

#include <boost/iostreams/device/mapped_file.hpp>

#include "Block.h"

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

  Task(int _num_input): 
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

  // TODO: use this function for both file read and HDFS read
  // possibly provide only one iostream as parameter
  virtual void readFromFile(DataBlock_ptr block, std::string path) { ; }

  DataBlock_ptr onDataReady(
      int64_t partition_id, 
      int length, int size, 
      std::string path) 
  {
    DataBlock_ptr block = input_table[partition_id];

    if (!block->isReady()) {

      if (length == -1) { // read from file

        // TODO: obtain iostream from HDFS or file

      }
      else {  // read from memory mapped file

        // allocate memory for block
        block->alloc(length, size);

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

  DataBlock_ptr createOutputBlock(int length, int data_width) {
    DataBlock_ptr block(new DataBlock(length, length*data_width));
    output_blocks.push_back(block);

    return block;
  }

  std::vector<DataBlock_ptr> input_blocks;
  std::vector<DataBlock_ptr> output_blocks;

private:
  int num_input;

  // number of input blocks that has data initialized
  int num_ready;

  std::map<int64_t, DataBlock_ptr> input_table;
};

}
#endif
