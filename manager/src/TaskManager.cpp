/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <boost/date_time/posix_time/posix_time.hpp>

#include "TaskManager.h"

namespace blaze {
#define LOG_HEADER  std::string("TaskManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")

Task* TaskManager::create() {
  
  Task* task = (Task*)createTask(env);

  task_queue.push(task);

  logger->logInfo(LOG_HEADER + std::string("created a new task"));

  return task;
}

void TaskManager::execute() {
  
  logger->logInfo(LOG_HEADER + std::string("started an executor"));

  // continuously execute tasks from the task queue
  while (1) { 

    // wait if there is no task to be executed
    while (task_queue.empty()) {
      boost::this_thread::sleep_for(boost::chrono::milliseconds(10)); 
    }

    // get next task and remove it from the task queue
    // this part is thread-safe with boost::lockfree::queue
    Task* task;
    task_queue.pop(task);

    // polling the status, wait for data to be transferred
    while (!task->isReady()) {
      boost::this_thread::sleep_for(boost::chrono::microseconds(100)); 
    }
    logger->logInfo(LOG_HEADER + std::string("Started a new task"));

    try {
      // start execution
      task->execute();
    } 
    catch (std::runtime_error &e) {
      logger->logErr(LOG_HEADER+ 
          std::string("task->execute() error: ")+
          e.what());
    }

    if (task->status == Task::FINISHED)  {

      // put task into the retire queue
      retire_queue.push(task);

      logger->logInfo(LOG_HEADER + std::string("Task finished"));
    }
    else { // task failed, retry 

      // TODO: add a retry counter in task
      //task_queue.push(task);

      logger->logInfo(LOG_HEADER + std::string(
            "Task failed due to previous error"));
    }
  }
}

void TaskManager::commit() {
    
  logger->logInfo(LOG_HEADER + std::string("started an committer"));
  // continuously retiring tasks from the retire queue
  while (1) {

    // wait if there is no task to be retired
    while (retire_queue.empty()) {
      boost::this_thread::sleep_for(boost::chrono::milliseconds(10)); 
    }

    // get the next task popped from the retire queue
    Task* task;
    retire_queue.pop(task);

    // polling the status, wait for output data to be read
    while (task->status != Task::COMMITTED) {
      boost::this_thread::sleep_for(boost::chrono::microseconds(10)); 
    }
    
    // deallocate the object using the function loaded from library
    destroyTask(task);

    logger->logInfo(LOG_HEADER + std::string("retired a task"));
  }
}

} // namespace blaze
