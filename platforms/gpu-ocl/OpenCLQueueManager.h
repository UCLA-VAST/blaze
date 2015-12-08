#ifndef OPENCL_QUEUE_MANAGER_H
#define OPENCL_QUEUE_MANAGER_H

#include "OpenCLCommon.h"
#include "QueueManager.h"
#include "TaskQueue.h"

namespace blaze {

class OpenCLQueueManager : public QueueManager {
public:

  OpenCLQueueManager(Platform* _platform);

  // start dispatch and executors for all GPU devices
  void startAll();

private:
  OpenCLPlatform* ocl_platform;

  // thread body of dispatching tasks from 
  // TaskQueue to PlatformQueue
  void do_dispatch();

  // thread body of PlatformQueue
  void do_execute(int device_id);

  // Platform Queues
  std::vector<TaskQueue_ptr> platform_queues;
};
} // namespace blaze

#endif
