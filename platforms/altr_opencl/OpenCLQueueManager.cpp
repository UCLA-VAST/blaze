#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/atomic.hpp>

#define LOG_HEADER "OpenCLQueueManager"
#include <glog/logging.h>

#include "blaze/Task.h"
#include "blaze/TaskManager.h"
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

  // start executor
  executors.create_thread(
      boost::bind(&OpenCLQueueManager::do_start, this));
}
OpenCLQueueManager::~OpenCLQueueManager() {
  // interrupt all executors
  executors.interrupt_all();
}

void OpenCLQueueManager::start() {
  // do nothing since the executors are already started
  DLOG(INFO) << "FPGAQueue started";
}

void OpenCLQueueManager::do_start() {
  
  OpenCLPlatform* ocl_platform = dynamic_cast<OpenCLPlatform*>(platform);

  if (!ocl_platform) {
    LOG(FATAL) << "Platform pointer incorrect";
  }
  VLOG(1) << "Start a executor for FPGAQueueManager";

  int retry_counter = 0;
  std::list<std::pair<std::string, TaskManager_ptr> > ready_queues;

  while (1) {
    if (queue_table.empty()) {
      // no ready queues at this point, sleep and check again
      boost::this_thread::sleep_for(boost::chrono::milliseconds(10)); 
      continue;
    }
    else {
      boost::lock_guard<QueueManager> guard(*this);

      // here a round-robin policy is enforced
      // iterate through all task queues
      if (ready_queues.empty()) {
        std::map<std::string, TaskManager_ptr>::iterator iter;
        for (iter = queue_table.begin();
            iter != queue_table.end();
            ++iter)
        {
          if (!iter->second->isEmpty()) {
            ready_queues.push_back(*iter);
          }
        }
      }
    }

    if (ready_queues.empty()) {
      // no ready queues at this point, sleep and check again
      boost::this_thread::sleep_for(boost::chrono::microseconds(1000)); 
      continue;
    }

    // select first queue
    std::string queue_name = ready_queues.front().first;
    TaskManager_ptr queue  = ready_queues.front().second;

    // switch bitstream for the selected queue
    try {
      ocl_platform->changeProgram(queue_name);
    }
    catch (std::runtime_error &e) {

      retry_counter++;

      if (retry_counter < 10) {
        LOG(WARNING) << "Programing bitstream failed " 
          << retry_counter << " times";
      }
      else {
        ocl_platform->removeQueue(queue_name);

        // remove queue_name from ready queue since it's already removed
        ready_queues.pop_front();

        // if setup program keeps failing, remove accelerator from queue_table 
        LOG(ERROR) << "Failed to setup bitstream for " << queue_name
          << ": " << e.what()
          << ". Remove it from QueueManager.";

        retry_counter = 0;
      }
      continue;
    }

    // timer to wait for the queue to fill up again
    int counter = 0;
    while (counter < reconfig_timer) {

      Task* task;
      if (queue->popReady(task)) {
        
        VLOG(1) << "Execute one task from " << queue_name;

        // execute one task
        try {
          uint64_t start_time = getUs();

          // start execution
          task->execute();

          // record task execution time
          uint64_t delay_time = getUs() - start_time;

          VLOG(1) << "Task finishes in " << delay_time << " us";
        } 
        catch (std::runtime_error &e) {
          LOG(ERROR) << "Task error " << e.what();
        }
        // reset the counter
        counter = 0;
      }
      else { 
        DLOG_EVERY_N(INFO, 50) << "Queue " << queue_name 
                               << " empty for " << counter << "ms";

        // start counter
        boost::this_thread::sleep_for(boost::chrono::milliseconds(1)); 
        
        counter++;
      }
    }
    // if the timer is up, switch to the next queue
    ready_queues.pop_front(); 
  }
}

} // namespace blaze
