#ifndef TASK_H
#define TASK_H

#include <string>
#include <map>
#include <vector>
#include <cstdlib>
#include <stdexcept>

#include "Common.h"

namespace blaze {

/**
 * Task is the base clase of an accelerator task
 * will be extended by user 
 */
class Task {
public:
  Task(int _num_args): 
    status(NOTREADY), 
    num_input(_num_args),
    num_ready(0)
  {; }

  // main function to be overwritten by accelerator implementations
  virtual void compute() {;}

  // wrapper around compute(), added indicator for task status
  void execute() {
    try {
      compute();
      status = FINISHED;
    } catch (std::exception &e) {
      status = FAILED; 
      throw std::runtime_error(e.what());
    }
  }
  
  // get config for input blocks
  std::string getConfig(int idx, std::string key);

  bool isInputReady(int64_t block_id);

protected:

  char* getOutput(int idx, int item_length, int num_items, int data_width);
  
  int getInputLength(int idx);

  int getInputNumItems(int idx);

  char* getInput(int idx);

  // add a configuration for a dedicated block 
  void addConfig(int idx, std::string key, std::string val);

  TaskEnv* getEnv();

  // a list of input blocks to its partition_id
  std::vector<int64_t> input_blocks;

  // list of output blocks
  std::vector<DataBlock_ptr> output_blocks;

  // a table that maps block index to configurations
  std::map<int, std::map<std::string, std::string> > config_table;

  // a mapping between partition_id to the input blocks
  std::map<int64_t, DataBlock_ptr> input_table;

private:

  // used by AppCommManager
  void addInputBlock(int64_t partition_id, DataBlock_ptr block);
  void inputBlockReady(int64_t partition_id, DataBlock_ptr block);

  // push one output block to consumer
  // return true if there are more blocks to output
  bool getOutputBlock(DataBlock_ptr &block);
   
  bool isReady();

  enum {
    NOTREADY,
    READY,
    FINISHED,
    FAILED,
    COMMITTED
  } status;

  // an unique id within each TaskQueue
  int task_id;

  // pointer to the TaskEnv
  TaskEnv_ptr env;

  // number of total input blocks
  int num_input;

  // number of input blocks that has data initialized
  int num_ready;
};
}
#endif
