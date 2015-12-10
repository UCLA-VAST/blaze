#ifndef OPENCL_QUEUE_MANAGER_H
#define OPENCL_QUEUE_MANAGER_H

#include "OpenCLCommon.h"
#include "QueueManager.h"

#define MAX_WAIT_TIME 500

namespace blaze {

class OpenCLQueueManager : public QueueManager {
public:

  OpenCLQueueManager(Platform* _platform);

  void startAll();

private:
  OpenCLPlatform* ocl_platform;

  // thread body of executing tasks from children TaskManagers
  void do_start();
};
} // namespace blaze

#endif
