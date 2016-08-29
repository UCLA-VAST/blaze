#include <stdio.h>
#include <fcntl.h>

#include <glog/logging.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "Task.h"
#include "Block.h"
#include "CommManager.h"
#include "BlockManager.h"
#include "TaskManager.h"
#include "Platform.h"
#include "PlatformManager.h"

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

    FLAGS_logtostderr = 1;
    google::InitGoogleLogging("");
    FLAGS_v = conf->verbose();

    // setup PlatformManager
    platform_manager = new PlatformManager(conf);

    // get correponding block managers 
    base_platform = platform_manager->getPlatformByAccId("base");
    test_platform = platform_manager->getPlatformByAccId("test");

    if (!base_platform) {
      throw std::runtime_error("failed to initialize base platform");
    }
    if (!test_platform) {
      throw std::runtime_error("failed to initialize test platform");
    }

    // get task managers
    base_tman = platform_manager->getTaskManager("base").lock();
    test_tman = platform_manager->getTaskManager("test").lock();

    if (!base_tman) {
      throw std::runtime_error("failed to get base acc");
    }
    if (!test_tman) {
      throw std::runtime_error("failed to get test acc");
    }
  }

  void setInput(
      int idx, 
      void* data, 
      int num_items, 
      size_t item_length,
      int data_width = sizeof(T)) {

    if (idx < input_base_blocks.size()) {
      input_base_blocks[idx]->writeData(data, num_items*item_length*data_width);
      input_test_blocks[idx]->writeData(data, num_items*item_length*data_width);
    }
    else {

      if (num_items == 1 && item_length == 1) {

        // create a new block and add it to input table
        DataBlock_ptr base_block(new DataBlock(num_items, 
              item_length, item_length*data_width));

        DataBlock_ptr test_block(new DataBlock(num_items, 
              item_length, item_length*data_width));

        base_block->writeData(data, num_items*item_length*data_width);
        test_block->writeData(data, num_items*item_length*data_width);     

        input_base_blocks.push_back(base_block);
        input_test_blocks.push_back(test_block);
      }
      else {
        int base_align_width = 0;
        int test_align_width = 0;

        if (!base_tman->getConfig(idx, "align_width").empty()) {
          base_align_width = stoi(base_tman->getConfig(idx, "align_width"));
        }
        if (!test_tman->getConfig(idx, "align_width").empty()) {
          test_align_width = stoi(test_tman->getConfig(idx, "align_width"));
        }

        DataBlock_ptr base_block = base_platform->createBlock(
            num_items, item_length, item_length*data_width, base_align_width);

        DataBlock_ptr test_block = test_platform->createBlock(
            num_items, item_length, item_length*data_width, test_align_width);

        base_block->writeData(data, num_items*item_length*data_width);
        test_block->writeData(data, num_items*item_length*data_width);     

        input_base_blocks.push_back(base_block);
        input_test_blocks.push_back(test_block);
      }
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
    Task_ptr task_base = base_tman->create();
    Task_ptr task_test = test_tman->create();
  
    for (int i=0; i<input_base_blocks.size(); i++) {

      // make sure that all the block IDs are different
      task_base->addInputBlock(2*i+0, input_base_blocks[i]);
      task_test->addInputBlock(2*i+1, input_test_blocks[i]);
    }

    base_tman->enqueue("testing", task_base.get());
    test_tman->enqueue("testing", task_test.get());
    
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
  bool checkResult(Task_ptr task_base, Task_ptr task_test) {
  
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

  PlatformManager* platform_manager;

  TaskManager_ptr base_tman;
  TaskManager_ptr test_tman;

  Platform* base_platform;
  Platform* test_platform;

  std::vector<DataBlock_ptr> input_base_blocks;
  std::vector<DataBlock_ptr> input_test_blocks;

};

}
