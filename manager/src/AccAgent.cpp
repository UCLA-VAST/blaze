#include <boost/filesystem.hpp>
#include <fstream>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>
#include <stdio.h>

#define LOG_HEADER "AccAgent"
#include <glog/logging.h>

#include "blaze/AccAgent.h"
#include "blaze/Block.h"
#include "blaze/CommManager.h"
#include "blaze/Platform.h"
#include "blaze/PlatformManager.h"
#include "blaze/TaskManager.h"
#include "proto/acc_conf.pb.h"

namespace blaze {

AccAgent::AccAgent(const char* conf_path) {
  int file_handle = open(conf_path, O_RDONLY);
  if (file_handle < 0) {
    throw fileError("AccAgent cannot find configure file");
  }
  ManagerConf_ptr conf(new ManagerConf());
  conf_ = conf;
  google::protobuf::io::FileInputStream fin(file_handle);
  if (!google::protobuf::TextFormat::Parse(&fin, conf.get())) {
    throw fileError("AccAgent cannot parse configuration file");
  }

  // Basic configurations
  //FLAGS_logtostderr = 1;
  FLAGS_v           = conf_->verbose();   // logging
  int app_port      = conf_->app_port();  // port for application
  int gam_port      = conf_->gam_port();  // port for GAM

  //google::InitGoogleLogging("");

  // Create local dir
  try {
    local_dir += "/nam-" + getUid();
    if (!boost::filesystem::exists(local_dir)) {
      boost::filesystem::create_directories(local_dir);
    }
    DLOG(INFO) << "Set 'local_dir' to " << local_dir;
  } catch (boost::filesystem::filesystem_error &e) {
    LOG(WARNING) << "Failed to use '" << local_dir 
                 << "' as local directory, using '/tmp' instead.";
  }

  // Create a PlatformManager
  PlatformManager_ptr platform_manager(new PlatformManager(conf_.get()));
  platform_manager_ = platform_manager;

  LOG(INFO) << "Finish initializing AccAgent";
}

Task_ptr AccAgent::createTask(std::string acc_id) {

  Task_ptr task;
  if (!platform_manager_) {
    DLOG(ERROR) << "Platform Manager not initialized.";
    return task;
  }
  Platform* platform = platform_manager_->getPlatformByAccId(acc_id);

  TaskManager_ref task_manager = platform_manager_->getTaskManager(acc_id);

  if (!task_manager.lock()) {
    DLOG(WARNING) << "Accelerator queue for " << acc_id
                  << " is likely to be removed";
    return task;
  }
  else {
    task = task_manager.lock()->create();
  }
  return task;
}

void AccAgent::writeInput(
    Task_ptr    task, 
    std::string acc_id,
    void*       data_ptr,
    int         num_items, 
    int         item_length, 
    int         data_width) 
{
  if (num_items <= 0 || 
      item_length <= 0 || 
      data_width <= 0)
  {
    DLOG(ERROR) << num_items << ", "
                << item_length << ", "
                << data_width;
    throw invalidParam("Invalid input parameters");
  }

  // create data block
  DataBlock_ptr block;

  if (num_items == 1 && item_length == 1) {
    if (data_width > 8) {
      LOG(WARNING) << "Scalar input cannot be larger than 8 bytes, "
                   << "force it to be 8 bytes";
      // since scalar variable uses a long long type in the message,
      // force data width to be 8 for safety
      data_width = 8; 
    }
    // create a normal block for scalar data
    DataBlock_ptr new_block(new DataBlock(num_items, 
          item_length, item_length*data_width));
    block = new_block;
  }
  else {
    Platform* platform = platform_manager_->getPlatformByAccId(acc_id);
    // create a platform block for normal input
    block = platform->createBlock(
        num_items, item_length, item_length*data_width);
  }
  block->writeData(data_ptr, num_items*item_length*data_width);

  // add the block to task's input list, block should be ready
  task->addInputBlock(task->input_blocks.size(), block);

  if (task->isReady()) {
    TaskManager_ref task_manager = platform_manager_->getTaskManager(acc_id);
    // skip the check on task_manager for now
    // use acc_id instead of app_id for application queue for efficiency
    task_manager.lock()->enqueue(acc_id, task.get());
  }
}

void AccAgent::readOutput(
    Task_ptr task, 
    void*    data_ptr,
    size_t   data_size) 
{
  // wait on task finish
  while (task->status != Task::FINISHED && 
         task->status != Task::FAILED) 
  {
    boost::this_thread::sleep_for(
        boost::chrono::microseconds(100)); 
  }
  if (task->status == Task::FINISHED) {
    VLOG(1) << "Task finished, starting to read output";

    DataBlock_ptr block;
    if (!task->getOutputBlock(block)) {
      throw internalError("No more output blocks");
    }
    block->readData(data_ptr, data_size);
  }
  else {
    throw internalError("Task failed");
  }
}
} // namespace blaze

