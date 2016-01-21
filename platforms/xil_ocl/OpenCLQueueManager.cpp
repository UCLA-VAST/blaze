#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/atomic.hpp>
#include <glog/logging.h>

#include "TaskManager.h"
#include "OpenCLPlatform.h"
#include "OpenCLQueueManager.h"

namespace blaze {

OpenCLQueueManager::OpenCLQueueManager(
    Platform* _platform,
    int _reconfig_timer
    ):
  QueueManager(_platform),
  reconfig_timer(_reconfig_timer)
{
  ocl_platform = dynamic_cast<OpenCLPlatform*>(platform);

  if (!ocl_platform) {
    LOG(ERROR) << "Platform pointer type is not OpenCLPlatform";
    throw std::runtime_error("Cannot create OpenCLQueueManager");
  }

  DLOG(INFO) << "Set FPGA reconfigure counter = " << _reconfig_timer;
}

void OpenCLQueueManager::startAll() {
  
  if (queue_table.size() == 0) {
    LOG(WARNING) << "No accelerator setup for the current platform";
  }
  else {
    boost::thread executor(
        boost::bind(&OpenCLQueueManager::do_start, this));
  }
}

void OpenCLQueueManager::do_start() {
  
  OpenCLPlatform* ocl_platform = dynamic_cast<OpenCLPlatform*>(platform);

  if (!ocl_platform) {
    LOG(FATAL) << "Platform pointer incorrect";
  }
  // NOTE: In current implementation dynamic accelerator registration
  // is not supported, so if there is only one accelerator skip the 
  // bitstream reprogramming logics
  if (queue_table.size() == 1) {

    std::string acc_id = queue_table.begin()->first;
    TaskManager_ptr task_manager = queue_table.begin()->second;

    DLOG(INFO) << "One accelerator on the platform, "
      << "Setup program and start TaskManager scheduler and executor";

    // program the device
    try {
      ocl_platform->changeProgram(acc_id);
    }
    catch (std::runtime_error &e) {

      // if setup program failed, remove accelerator from queue_table 
      LOG(ERROR) << "Failed to setup bitstream for " << acc_id 
        << ": " << e.what()
        << ". Remove it from QueueManager.";
      queue_table.erase(queue_table.find(acc_id));

      return;
    }

    task_manager->start();
  }
  else {

    // start the scheduler for each queue;
    std::map<std::string, TaskManager_ptr>::iterator iter;
    for (iter = queue_table.begin();
        iter != queue_table.end();
        ++iter)
    {
      iter->second->startScheduler();
    }

    std::list<std::string> ready_queues;
    while (1) {

      // here a round-robin policy is enforced
      if (ready_queues.empty()) {
        std::map<std::string, TaskManager_ptr>::iterator iter;
        for (iter = queue_table.begin();
            iter != queue_table.end();
            ++iter)
        {
          int queue_length = iter->second->getExeQueueLength();
          if (queue_length > 0) {
            ready_queues.push_back(iter->first);
          }
        }
      }

      if (ready_queues.empty()) {
        // no ready queues at this point, sleep and check again
        boost::this_thread::sleep_for(boost::chrono::microseconds(1000)); 
      }
      else {
        // select first queue
        std::string queue_name = ready_queues.front();

        // switch bitstream for the selected queue
        try {
          ocl_platform->changeProgram(queue_name);
        }
        catch (std::runtime_error &e) {

          // if setup program failed, remove accelerator from queue_table 
          LOG(ERROR) << "Failed to setup bitstream for " << queue_name
            << ": " << e.what()
            << ". Remove it from QueueManager.";
          queue_table.erase(queue_table.find(queue_name));

          continue;
        }

        TaskManager_ptr queue = queue_table[queue_name];

        // timer to wait for the queue to fill up again
        int counter = 0;
        while (counter < reconfig_timer) {
          if (queue->getExeQueueLength() > 0) {

            counter = 0;
            
            VLOG(1) << "Execute one task from " << queue_name;

            // execute one task
            queue->execute();
          }
          else { 
            DLOG_EVERY_N(INFO, 50) << "Queue " << queue_name << " empty for 50ms";

            // start counter
            boost::this_thread::sleep_for(boost::chrono::milliseconds(1)); 
            
            counter++;
          }
        }
        // if the timer is up, switch to the next queue
        ready_queues.pop_front(); 
      }
    }
  }
}

} // namespace blaze
