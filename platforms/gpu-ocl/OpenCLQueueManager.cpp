#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/atomic.hpp>
#include <glog/logging.h>

#include "Task.h"
#include "TaskManager.h"
#include "OpenCLEnv.h"
#include "OpenCLBlock.h"
#include "OpenCLPlatform.h"
#include "OpenCLQueueManager.h"

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
}

void OpenCLQueueManager::startAll() {
  
  if (queue_table.size() == 0) {
    LOG(WARNING) << "No accelerator setup for the current platform";
  }
  else {
    // start the scheduler for each TaskManager
    std::map<std::string, TaskManager_ptr>::iterator iter;
    for (iter = queue_table.begin();
        iter != queue_table.end();
        ++iter)
    {
      iter->second->startScheduler();
    }
    boost::thread dispatcher(
        boost::bind(&OpenCLQueueManager::do_dispatch, this));

    for (int d=0; d<platform_queues.size(); d++) {
      boost::thread executor(
        boost::bind(&OpenCLQueueManager::do_execute, this, d));
    }
  }
}

void OpenCLQueueManager::do_dispatch() {

  VLOG(1) << "Start a dispatcher for GPU Queue Manager";

  while (1) {

    // NOTE: no reprogramming optimization, assuming GPU 
    // reprogramming cost is trivial

    bool allEmpty = true;

    // iterate through all task queues
    std::map<std::string, TaskManager_ptr>::iterator iter;
    for (iter = queue_table.begin();
        iter != queue_table.end();
        ++iter)
    {
      Task* task;
      bool taskReady = iter->second->popReady(task);

      if (taskReady) { 
        if (!task) {
          DLOG(ERROR) << "Unexpected NULL Task pointer";
          continue;
        }
        allEmpty = false;

        // get the task env to query device assignment
        OpenCLTaskEnv* taskEnv = 
          dynamic_cast<OpenCLTaskEnv*>(getTaskEnv(task));
        if (!taskEnv) {
          DLOG(ERROR) << "TaskEnv pointer NULL";
          continue;
        }

        // get the block env of first input
        // NOTE: here use input idx=0, assuming that is the 
        // main input data of the task
        DataBlock* block = getTaskInputBlock(task, 0).get();
        OpenCLBlock* ocl_block = dynamic_cast<OpenCLBlock*>(block);
        if (!ocl_block) {
          DLOG(ERROR) << "Block is not of type OpenCLBlock";
          continue; 
          // TODO: fail task
        }
        OpenCLEnv* blockEnv = ocl_block->env;

        // query device assignment based on task env and block env
        int taskLoc = taskEnv->env->getDeviceId();
        int blockLoc = blockEnv->getDeviceId();

        // assign task based on the block location
        // NOTE: here there could be additional load balancing
        int queueLoc = blockLoc;
        DLOG(INFO) << "Assigned task to GPU_" << blockLoc;

        // switch task environment to match the block device
        taskEnv->env = blockEnv;

        if (queueLoc < platform_queues.size()) {
          platform_queues[queueLoc]->push(task);
        }
      }
    }

    if (allEmpty) {
      // no ready queues at this point, sleep and check again
      boost::this_thread::sleep_for(boost::chrono::microseconds(1000)); 
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
