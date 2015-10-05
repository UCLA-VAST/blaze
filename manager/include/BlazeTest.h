#include <fcntl.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "../src/Task.h"
#include "../src/Block.h"
#include "../src/CommManager.h"
#include "../src/BlockManager.h"
#include "../src/TaskManager.h"
#include "../src/PlatformManager.h"

namespace blaze {

// test two accelerator implementations of 
// input type T and output type U
template <typename T, typename U> 
class BlazeTest {

public:
  BlazeTest(
      char* conf_path, 
      U eps): thresh(eps)
  {
    int file_handle = open(conf_path, O_RDONLY);
    if (file_handle < 0) {
      throw std::runtime_error("cannot find configure file "+
          std::string(conf_path));
    }
    ManagerConf *conf = new ManagerConf();
    google::protobuf::io::FileInputStream fin(file_handle);

    if (!google::protobuf::TextFormat::Parse(&fin, conf)) {
      throw std::runtime_error("cannot parse configuration from "+
          std::string(conf_path));  
    }

    // setup Logger
    int verbose = conf->verbose();  
    logger = new Logger(verbose);

    // setup PlatformManager
    platform_manager = new PlatformManager(conf, logger);

    // get task managers
    base_tman = platform_manager->getTaskManager("base");
    test_tman = platform_manager->getTaskManager("test");

    // get correponding block managers 
    base_bman = platform_manager->getBlockManager("base");
    test_bman = platform_manager->getBlockManager("test");

    if (!base_bman || !test_bman) {
      throw std::runtime_error("failed to initialize both accelerators");
    }
  }

  void setInput(int idx, void* data, int num_items, size_t length) {

    if (idx < input_base_blocks.size()) {
      input_base_blocks[idx]->writeData(data, length*sizeof(T));
      input_test_blocks[idx]->writeData(data, length*sizeof(T));
    }
    else {
      DataBlock_ptr base_block = base_bman->create(length, length*sizeof(T));
      DataBlock_ptr test_block = test_bman->create(length, length*sizeof(T));

      base_block->setNumItems(num_items);
      test_block->setNumItems(num_items);

      base_block->writeData(data, length*sizeof(T));
      test_block->writeData(data, length*sizeof(T));

      input_base_blocks.push_back(base_block);
      input_test_blocks.push_back(test_block);
    }
  }

  void runTest() {
    Task* task_test = test_tman->create();

    for (int i=0; i<input_base_blocks.size(); i++) {

      task_test->addInputBlock(2*i+1, input_test_blocks[i]);
    }
    // wait on task finish
    while (
        task_test->status != Task::FINISHED &&
        task_test->status != Task::FAILED) 
    {
      boost::this_thread::sleep_for(
          boost::chrono::microseconds(10)); 
    }

    if (task_test->status != Task::FINISHED) {
      throw std::runtime_error("Task failed.");
    }
  }

  void run() {
    // create new tasks
    Task* task_base = base_tman->create();
    Task* task_test = test_tman->create();
  
    for (int i=0; i<input_base_blocks.size(); i++) {

      // make sure that all the block IDs are different
      task_base->addInputBlock(2*i+0, input_base_blocks[i]);
      task_test->addInputBlock(2*i+1, input_test_blocks[i]);
    }
    
    // wait on task finish
    while (
        (task_base->status != Task::FINISHED &&
         task_base->status != Task::FAILED ) ||
        (task_test->status != Task::FINISHED &&
        task_test->status != Task::FAILED)) 
    {
      boost::this_thread::sleep_for(
          boost::chrono::microseconds(10)); 
    }

    if (task_base->status != Task::FINISHED ||
        task_test->status != Task::FINISHED) 
    {
      throw std::runtime_error("Task failed.");
    }

    // tasks finished, check results
    if (checkResult(task_base, task_test)) {
      printf("Results correct.\n");
    }
    else {
      printf("Results incorrect.\n");
    }
  }

private:
  bool checkResult(Task* task_base, Task* task_test) {
  
    DataBlock_ptr output_base;
    DataBlock_ptr output_test;

    // NOTE: assuming single output block
    task_base->getOutputBlock(output_base);
    task_test->getOutputBlock(output_test);

    U* result_base = new U[output_base->getSize()];
    U* result_test = new U[output_test->getSize()];

    output_base->readData(result_base, output_base->getSize());
    output_test->readData(result_test, output_test->getSize());

    // post first 10 wrong results
    int counter = 0;
    U diff_total = 0.0;
    U max_diff = 0.0;
    for (int k=0; k<output_base->getLength(); k++) {
      U diff = std::abs(result_base[k] - result_test[k]); 
      U diff_ratio = 0.0;
      if (result_base[k]!=0) {
        diff_ratio = diff / std::abs(result_base[k]);
      }
      if (diff_ratio > max_diff) {
        max_diff = diff_ratio;
      }
      if (diff_ratio > 1e-5 && counter<10) {
        if (counter==0) {
          printf("First 10 mismatch results: expected | actual, ratio\n");
        }
        printf("%f|%f, ratio=%f\n", 
            result_base[k], result_test[k], diff_ratio);
        counter ++;
      }
    }
    delete result_base;
    delete result_test;

    if (max_diff>thresh) {
      return false;
    }
    else {
      return true;
    }
  }

  // threshold for difference
  U thresh;

  Logger *logger;

  PlatformManager* platform_manager;

  TaskManager_ptr base_tman;
  TaskManager_ptr test_tman;

  BlockManager* base_bman;
  BlockManager* test_bman;

  std::vector<DataBlock_ptr> input_base_blocks;
  std::vector<DataBlock_ptr> input_test_blocks;

};

}
