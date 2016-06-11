#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/atomic.hpp>
#include <glog/logging.h>

#include "blaze/TaskEnv.h"
#include "blaze/Task.h"
#include "blaze/Block.h"
#include "blaze/TaskQueue.h"
#include "blaze/TaskManager.h"
#include "blaze/Platform.h"

namespace blaze {

bool TaskManager::isEmpty() {
  return execution_queue.empty();
}

Task_ptr TaskManager::create() {
  
  // create a new task by the constructor loaded form user implementation
  Task_ptr task(createTask(), destroyTask);

  // link the TaskEnv
  task->env = platform->getEnv(acc_id);

  // give task an unique ID
  task->task_id = nextTaskId.fetch_add(1);

  return task;
}

void TaskManager::enqueue(std::string app_id, Task* task) {

  if (!task->isReady()) {
    throw std::runtime_error("Cannot enqueue task that is not ready");
  }
  
  // TODO: when do we remove the queue?
  // create a new app queue if it does not exist

  TaskQueue_ptr queue;
  // TODO: remove this lock
  this->lock();
  if (app_queues.find(app_id) == app_queues.end()) {
    TaskQueue_ptr new_queue(new TaskQueue());
    app_queues.insert(std::make_pair(app_id, new_queue));
    queue = new_queue;
  } 
  else {
    queue = app_queues[app_id];
  }
  this->unlock();

  // push task to queue
  bool enqueued = queue->push(task);
  while (!enqueued) {
    boost::this_thread::sleep_for(boost::chrono::microseconds(100)); 
    enqueued = queue->push(task);
  }
}

bool TaskManager::schedule() {

  if (app_queues.empty()) {
    return true;
  }
  // iterate through all app queues and record which are non-empty
  std::vector<std::string> ready_queues;
  std::map<std::string, TaskQueue_ptr>::iterator iter;

  this->lock();
  for (iter = app_queues.begin();
      iter != app_queues.end();
      iter ++)
  {
    if (iter->second && !iter->second->empty()) {
      ready_queues.push_back(iter->first);
    }
  }
  this->unlock();
  if (ready_queues.empty()) {
    return true;
  }

  Task* next_task;

  // select the next task to execute from application queues
  // use RoundRobin scheduling
  int idx_next = rand()%ready_queues.size();

  if (app_queues.find(ready_queues[idx_next]) == app_queues.end()) {
    LOG(ERROR) << "Did not find app_queue, unexpected";
    return false;
  }
  else {
    app_queues[ready_queues[idx_next]]->pop(next_task);
  }
  if (next_task) {
    execution_queue.push(next_task);

    VLOG(1) << "Schedule a task to execute from " << ready_queues[idx_next];
  }

  return false;
}

// return true if the execution queue is empty
bool TaskManager::execute() {

  // wait if there is no task to be executed
  if (execution_queue.empty()) {
    return true;
  }
  // get next task and remove it from the task queue
  // this part is thread-safe with boost::lockfree::queue
  Task* task;
  execution_queue.pop(task);

  VLOG(1) << "Started a new task";

  try {
    // record task execution time
    uint64_t start_time = getUs();

    // start execution
    task->execute();
    uint64_t delay_time = getUs() - start_time;

    VLOG(1) << "Task finishes in " << delay_time << " us";
  } 
  catch (std::runtime_error &e) {
    LOG(ERROR) << "Task error " << e.what();
  }
  return false;
}

bool TaskManager::popReady(Task* &task) {
  if (execution_queue.empty()) {
    return false;
  }
  else {
    execution_queue.pop(task);
    return true;
  }
}

std::string TaskManager::getConfig(int idx, std::string key) {
  Task* task = (Task*)createTask();

  std::string config = task->getConfig(idx, key);

  destroyTask(task);
  
  return config;
}

void TaskManager::do_execute() {

  VLOG(1) << "Started an executor for " << acc_id;

  // continuously execute tasks from the task queue
  while (power || !scheduler_idle || !executor_idle) { 
    executor_idle = false;
    executor_idle = execute();
    if (executor_idle) {
      boost::this_thread::sleep_for(boost::chrono::microseconds(100)); 
    }
  }

  VLOG(1) << "Executor for " << acc_id << " stopped";
}

void TaskManager::do_schedule() {
  
  VLOG(1) << "Started an scheduler for " << acc_id;

  while (power || !scheduler_idle) {
    // return true if the app queues are all empty
    scheduler_idle = false;
    scheduler_idle = schedule();
    if (scheduler_idle) {
      boost::this_thread::sleep_for(boost::chrono::microseconds(100));
    }
  }

  VLOG(1) << "Scheduler for " << acc_id << " stopped";
}

bool TaskManager::isBusy() {
  return !scheduler_idle || !executor_idle;
}

void TaskManager::start() {
  startExecutor();
  startScheduler();
}

void TaskManager::stop() {
  power = false;
}

void TaskManager::startExecutor() {
  task_workers.create_thread(
      boost::bind(&TaskManager::do_execute, this));
}

void TaskManager::startScheduler() {
  task_workers.create_thread(
      boost::bind(&TaskManager::do_schedule, this));
}

} // namespace blaze
