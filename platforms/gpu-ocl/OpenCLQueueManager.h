#ifndef OPENCL_QUEUE_MANAGER_H
#define OPENCL_QUEUE_MANAGER_H

#include "QueueManager.h"

namespace blaze {

class OpenCLQueueManager : public QueueManager {
public:

  OpenCLQueueManager(Platform* _platform)
    : QueueManager(_platform),
      batch_size(8)
  {;}

  void startAll();

private:
  // thread body of executing tasks from children TaskManagers
  void do_start();

  int batch_size;
};
} // namespace blaze

#endif
