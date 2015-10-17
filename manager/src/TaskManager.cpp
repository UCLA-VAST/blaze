#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/atomic.hpp>

#include "TaskManager.h"

namespace blaze {
#define LOG_HEADER  std::string("TaskManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")

int TaskManager::estimateTime(Task* task) {

  // check if time estimation is already stored
  if (task->estimated_time > 0) {
    return task->estimated_time;
  }
  else {
    int task_delay = task->estimateTime();
    int ret_delay = 0;

    if (task_delay <= 0) { // no estimation
      // TODO: use a regression model
      // current implementation just return a constant
      ret_delay = 1e5;
    }
    else {
      // TODO: apply an estimation model (linear regression)
      ret_delay = task_delay + deltaDelay;
    }
    // store the time estimation in task
    task->estimated_time = ret_delay;

    return ret_delay;
  }
}

void TaskManager::updateDelayModel(
    Task* task, 
    int estimateTime, int realTime) 
{
  ;
}

Task_ptr TaskManager::create() {
  
  // create a new task by the constructor loaded form user implementation
  Task_ptr task(createTask(), destroyTask);

  // link the task platform 
  task->setPlatform(platform);

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
  if (app_queues.find(app_id) == app_queues.end()) {
    TaskQueue_ptr queue(new TaskQueue());
    app_queues.insert(std::make_pair(app_id, queue));
  }
  // once called, the task estimation time will stored
  int delay_time = estimateTime(task);

  // push task to queue
  while (!app_queues[app_id]->push(task)) {
    boost::this_thread::sleep_for(boost::chrono::microseconds(100)); 
  }

  // update lobby wait time
  lobbyWaitTime.fetch_add(delay_time);

  // update door wait time
  doorWaitTime.fetch_sub(delay_time);
}

void TaskManager::schedule() {
  
  logger->logInfo(LOG_HEADER + std::string("started a scheduler"));

  while (power) {
    // iterate through all app queues and record which are non-empty
    std::vector<std::string> ready_queues;
    std::map<std::string, TaskQueue_ptr>::iterator iter;
    for (iter = app_queues.begin();
         iter != app_queues.end();
         iter ++)
    {
      if (!iter->second->empty()) {
        ready_queues.push_back(iter->first);
      }
    }

    if (ready_queues.empty()) {
      boost::this_thread::sleep_for(boost::chrono::microseconds(100)); 
    }
    else {
      Task* next_task;

      // select the next task to execute from application queues
      // use RoundRobin scheduling
      int idx_next = rand()%ready_queues.size();

      app_queues[ready_queues[idx_next]]->pop(next_task);

      logger->logInfo(LOG_HEADER+
          std::string("Schedule a task to execute from ")+
          ready_queues[idx_next]);

      execution_queue.push(next_task);
    }
  }
}

void TaskManager::execute() {
  
  logger->logInfo(LOG_HEADER + std::string("started an executor"));

  // continuously execute tasks from the task queue
  while (power) { 

    // wait if there is no task to be executed
    if (execution_queue.empty()) {
      boost::this_thread::sleep_for(boost::chrono::microseconds(100)); 
    }
    else {
      // get next task and remove it from the task queue
      // this part is thread-safe with boost::lockfree::queue
      Task* task;
      execution_queue.pop(task);

      int delay_estimate = estimateTime(task);

      logger->logInfo(LOG_HEADER + std::string("Started a new task"));

      // record task execution time
      uint64_t start_time = logger->getUs();
      try {
        // start execution
        task->execute();
      } 
      catch (std::runtime_error &e) {
        logger->logErr(LOG_HEADER+ 
            std::string("task->execute() error: ")+
            e.what());
      }
      uint64_t delay_time = logger->getUs() - start_time;
      logger->logInfo(LOG_HEADER + std::string("Task finishes in ")+
          std::to_string((long long) delay_time) + std::string(" us"));

      // if the task is successful, update delay estimation model
      if (task->status == Task::FINISHED) {
        updateDelayModel(task, delay_estimate, delay_time);
      }

      // decrease the waittime, use the recorded estimation 
      lobbyWaitTime.fetch_sub(delay_estimate);
    }
  }
}

std::pair<int, int> TaskManager::getWaitTime(Task* task) {

  // increment door with current task time
  int currDoorWaitTime  = doorWaitTime.fetch_add(estimateTime(task));
  int currLobbyWaitTime = lobbyWaitTime.load();

  return std::make_pair(
      currLobbyWaitTime, currLobbyWaitTime + currDoorWaitTime);
}

std::string TaskManager::getConfig(int idx, std::string key) {
  Task* task = (Task*)createTask();

  std::string config = task->getConfig(idx, key);

  destroyTask(task);
  
  return config;
}

void TaskManager::start() {
  power = true;
  
  boost::thread scheduler(
      boost::bind(&TaskManager::schedule, this));

  boost::thread executor(
      boost::bind(&TaskManager::execute, this));
}

} // namespace blaze
