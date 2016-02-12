#ifndef OPENCL_QUEUE_MANAGER_H
#define OPENCL_QUEUE_MANAGER_H

#include "OpenCLCommon.h"
#include "QueueManager.h"

namespace blaze {

class OpenCLQueueManager : public QueueManager {
public:

  OpenCLQueueManager(
      Platform* _platform,
      int _reconfig_timer = 500);

  void startAll();

private:
  OpenCLPlatform* ocl_platform;

  // thread body of executing tasks from children TaskManagers
  void do_start();

  int reconfig_timer;
};
} // namespace blaze

#endif
