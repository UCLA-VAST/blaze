#ifndef TASK_QUEUE_H
#define TASK_QUEUE_H

#include <boost/lockfree/queue.hpp>

#include "Common.h"

namespace blaze {

class TaskQueue {

public: 

  TaskQueue() {;}

  bool empty() {
    return task_queue.empty();
  }

  bool push(Task* task) {
    return task_queue.push(task);
  }

  bool pop(Task* &task) {
    if ( empty() ) {
      return false;
    }
    else {
      bool status = task_queue.pop(task);

      return status;
    }
  }

private:
  boost::lockfree::queue<Task*, boost::lockfree::capacity<256> > task_queue;
};

typedef boost::shared_ptr<TaskQueue> TaskQueue_ptr;
} // namespace blaze
#endif
