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

#include "Block.h"
#include "Platform.h"

namespace blaze {

// forward declaration of 
class TaskManager;
class AppCommManager;
template <typename U, typename T> class BlazeTest;

/**
 * Task is the base clase of an accelerator task
 * will be extended by user 
 */
class Task {

friend class TaskManager;
friend class AppCommManager;
template <typename U, typename T> 
friend class BlazeTest;

public:
  Task(int _num_input): 
    status(NOTREADY), 
    estimated_time(-1),
    num_input(_num_input),
    num_ready(0)
  {; }

  virtual int estimateTime() { return -1; }
  virtual int estimateSpeedup() { return -1; }

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
  
  // get config for input blocks
  // TODO: need a way to specify general configs
  // or config for output block
  std::string getConfig(int idx, std::string key);

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

  TaskEnv* getEnv();

  char* getOutput(int idx, int item_length, int num_items, int data_width);
  
  int getInputLength(int idx);
  int getInputNumItems(int idx);
  char* getInput(int idx);

  // add a configuration for a dedicated block 
  void addConfig(int idx, std::string key, std::string val);

private:

  // used by CommManager
  void addInputBlock(int64_t partition_id, DataBlock_ptr block);
  void inputBlockReady(int64_t partition_id, DataBlock_ptr block);

  DataBlock_ptr getInputBlock(int64_t block_id);

  // push one output block to consumer
  // return true if there are more blocks to output
  bool getOutputBlock(DataBlock_ptr &block);
   
  void setPlatform(Platform *_platform) { platform = _platform;  }

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

  int estimated_time;

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
