#ifndef TASK_QUEUE_H
#define TASK_QUEUE_H

#include <boost/lockfree/queue.hpp>

#include "Task.h"

namespace blaze {

class TaskQueue {

public: 

  TaskQueue() {;}

  bool empty() {
    return task_queue.empty() || delay_queue.empty(); 
  }

  bool push(std::pair<Task*, int> task_pair) {
    return push(task_pair.first, task_pair.second);
  }

  bool push(Task* task, int delay) {
    bool status = task_queue.push(task);
    status = status &&delay_queue.push(delay);
    if (!status) {
      return false;
    } else {
      return true;
    }
  }

  bool pop(std::pair<Task*, int> &task_pair) {
      Task* task;
      int delay;

      bool status = pop(task, delay);

      task_pair.first = task;
      task_pair.second = delay;

      return status;
  }

  bool pop(Task* &task, int &delay) {
    if ( empty() ) {
      return false;
    }
    else {
      bool status = task_queue.pop(task);
      status = status && delay_queue.pop(delay);

      return status;
    }
  }

private:
  boost::lockfree::queue<Task*, boost::lockfree::capacity<256> > task_queue;
  boost::lockfree::queue<int, boost::lockfree::capacity<256> > delay_queue;
};

typedef boost::shared_ptr<TaskQueue> TaskQueue_ptr;
} // namespace blaze
#endif
