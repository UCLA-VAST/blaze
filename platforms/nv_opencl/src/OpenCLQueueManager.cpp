#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/atomic.hpp>

#define LOG_HEADER "OpenCLQueueManager"
#include <glog/logging.h>

#include "blaze/Task.h"
#include "blaze/TaskManager.h"
#include "blaze/nv_opencl/OpenCLBlock.h"
#include "blaze/nv_opencl/OpenCLEnv.h"
#include "blaze/nv_opencl/OpenCLPlatform.h"
#include "blaze/nv_opencl/OpenCLQueueManager.h"

namespace blaze {

OpenCLQueueManager::OpenCLQueueManager(Platform* _platform):
  QueueManager(_platform) 
{
  ocl_platform = dynamic_cast<OpenCLPlatform*>(platform);

  if (!ocl_platform) {
    LOG(ERROR) << "Platform pointer type is not OpenCLPlatform";
    throw std::runtime_error("Cannot create OpenCLQueueManager");
  }

  // allocate platform queues
  int num_devices = ocl_platform->getNumDevices();

  for (int d=0; d<num_devices; d++) {
    TaskQueue_ptr queue(new TaskQueue());
    platform_queues.push_back(queue);
  }

  // start dispatcher
  boost::thread dispatcher(
      boost::bind(&OpenCLQueueManager::do_dispatch, this));

  // start executor
  for (int d=0; d<platform_queues.size(); d++) {
    boost::thread executor(
        boost::bind(&OpenCLQueueManager::do_execute, this, d));
    }
}

void OpenCLQueueManager::start(std::string id) {
  // do nothing since the executors are already started
}

void OpenCLQueueManager::do_dispatch() {

  VLOG(1) << "Start a dispatcher for GPU Queue Manager";

  while (1) {

    // NOTE: no reprogramming optimization, assuming GPU 
    // reprogramming cost is trivial

    std::vector<Task*> readyTasks;

    if (queue_table.empty()) {
      // no ready queues at this point, sleep and check again
      boost::this_thread::sleep_for(boost::chrono::milliseconds(10)); 
      continue;
    }
    else {
      boost::lock_guard<QueueManager> guard(*this);

      // iterate through all task queues
      std::map<std::string, TaskManager_ptr>::iterator iter;
      for (iter = queue_table.begin();
          iter != queue_table.end();
          ++iter)
      {
        Task* task = NULL;
        bool taskReady = iter->second->popReady(task);

        if (taskReady && task) {
          readyTasks.push_back(task);
        }
      }
    }

    if (readyTasks.empty()) {
      // no ready queues at this point, sleep and check again
      boost::this_thread::sleep_for(boost::chrono::microseconds(1000)); 
      continue;
    }

    for (std::vector<Task*>::iterator iter = readyTasks.begin();
         iter != readyTasks.end();
         iter++) 
    {
      Task* task = *iter;

      // get the task env to query device assignment
      OpenCLTaskEnv* taskEnv = 
        dynamic_cast<OpenCLTaskEnv*>(getTaskEnv(task));
      if (!taskEnv) {
        DLOG(ERROR) << "TaskEnv pointer NULL";
        continue;
      }

      // decide the device assignment based on input block
      // NOTE: here use input idx=0, assuming that is the 
      // main input data of the task
      DataBlock_ptr block = getTaskInputBlock(task, 0);
      OpenCLBlock* ocl_block = dynamic_cast<OpenCLBlock*>(block.get());
      if (!ocl_block) {
        DLOG(ERROR) << "Block is not of type OpenCLBlock";
        continue; 
        // TODO: fail task
      }
      // query device assignment based on task env and block env
      int blockLoc = ocl_block->getDeviceId();

      // assign task based on the block location
      // NOTE: here there could be additional load balancing
      int queueLoc = blockLoc;

      // switch task environment to match the block device
      taskEnv->relocate(queueLoc);

      // assign task input blocks to the device;
      int block_idx = 1;
      while (block != NULL_DATA_BLOCK) {
        block = getTaskInputBlock(task, block_idx); 
        OpenCLBlock* ocl_block = dynamic_cast<OpenCLBlock*>(block.get());

        if (ocl_block) {
          // other types of data block will not be considered
          if (ocl_block->getFlag() == BLAZE_SHARED_BLOCK) 
          {
            OpenCLBlock* new_block = new OpenCLBlock(*ocl_block);
            new_block->relocate(queueLoc);

            DataBlock_ptr bp(new_block);   

            setTaskInputBlock(task, bp, block_idx);
          } 
        }
        block_idx++;
      }

      if (queueLoc < platform_queues.size()) {
        platform_queues[queueLoc]->push(task);
      }
    }
  }
}

void OpenCLQueueManager::do_execute(int device_id) {

  VLOG(1) << "Started an executor for GPU_" << device_id;

  TaskQueue_ptr queue = platform_queues[device_id];
  
  while (1) {
    // wait if there is no task to be executed
    if (queue->empty()) {
      boost::this_thread::sleep_for(boost::chrono::microseconds(100)); 
    }
    else {
      VLOG(1) << "<GPU_" << device_id<< "> Started a new task";

      try {
        Task* task;
        queue->pop(task);

        OpenCLTaskEnv* taskEnv = 
          dynamic_cast<OpenCLTaskEnv*>(getTaskEnv(task));

        // record task execution time
        uint64_t start_time = getUs();

        // start execution
        task->execute();
        uint64_t delay_time = getUs() - start_time;

        VLOG(1) << "<GPU_" << device_id<< "> Task finishes in " 
          << delay_time << " us";
      } 
      catch (std::runtime_error &e) {
        LOG(ERROR) << "Task error " << e.what();
      }
    }
  }
}

} // namespace blaze
