#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/atomic.hpp>

#define "OpenCLQueueManager"
#include <glog/logging.h>

#include "TaskManager.h"
#include "OpenCLPlatform.h"
#include "OpenCLQueueManager.h"

namespace blaze {

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

    try {
      ocl_platform->setupProgram(acc_id);
    }
    catch (std::exception &e) {
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
        // select first queue and remove it from list
        std::string queue_name = ready_queues.front();

        // switch bitstream for the selected queue
        try {
          ocl_platform->setupProgram(queue_name);
        }
        catch (std::runtime_error &e) {

          // if setup program failed, remove accelerator from queue_table 
          LOG(ERROR) << "Failed to setup bitstream for " << queue_name
            << ": " << e.what()
            << ". Remove it from QueueManager.";
          queue_table.erase(queue_table.find(queue_name));

          break;
        }

        TaskManager_ptr queue = queue_table[queue_name];

        // start a batch of executions
        int b;
        for (b=0; b<batch_size; b++) {
          if (queue->getExeQueueLength() > 0) {
            DLOG(INFO) << "Execute one task from " << queue_name;

            // execute one task
            queue->execute();
          }
          else {
            if (ready_queues.size() > 1) {
              // wait for new tasks for the current acc
              boost::this_thread::sleep_for(boost::chrono::milliseconds(200)); 

              // only switch acc if queue stays empty
              if (queue->getExeQueueLength()==0) {
                ready_queues.pop_front(); 
              }
            }
            else {
              ready_queues.pop_front(); 
            }
            break; 
          }
        }

        // modify batch size to reduce reprogramming
        // TODO: disabled for now
        if (b<batch_size) {
          DLOG(INFO) << "Batch not finished, executed " << 
            b << " out of " <<
            batch_size << " tasks.";

          // reduce the batch size if batch not finished
          ///if (batch_size > 4) {
          ///  batch_size = batch_size / 2;
          ///}
        }
        else {
          DLOG(INFO) << "Batch of size " << batch_size << " finished";

          // increase batch 
          //batch_size = batch_size * 2;
        }
      }
    }
  }
}

} // namespace blaze
