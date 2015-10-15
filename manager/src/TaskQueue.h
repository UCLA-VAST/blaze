#ifndef TASK_QUEUE_H
#define TASK_QUEUE_H

#include <boost/lockfree/queue.hpp>

#include "Task.h"

namespace blaze {

class TaskQueue {

public: 

  TaskQueue() {;}

  bool empty() {
    return task_queue.empty();
  }

  bool push(Task* task) {
    bool status = task_queue.push(task);
    if (!status) {
      return false;
    } else {
      return true;
    }
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
