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

#ifndef CONTEXT_H
#define CONTEXT_H

#include <string>
#include <vector>
#include <map>

#include <boost/smart_ptr.hpp>

#include "acc_conf.pb.h"

#include "BlockManager.h"
#include "QueueManager.h"
#include "TaskManager.h"
#include "Logger.h"
#include "TaskEnv.h"
#include "Block.h"

#ifdef USE_OPENCL
#include "OpenCLEnv.h"
#include "OpenCLBlock.h"
#endif

namespace blaze {

class Context {

public:
  Context(Logger* _logger): logger(_logger) {;}
  
  Context(ManagerConf *conf,
      Logger *_logger,
      QueueManager *_manager);

  TaskEnv* getEnv(std::string acc_id) {
     if (acc_table.find(acc_id) != acc_table.end()) {
      return acc_table[acc_id].get();  
    }
    else {
      return NULL;
    }
  }

  TaskEnv* getEnv(AccType type) {
    if (env_table.find(type) != env_table.end()) {
      return env_table[type].get();  
    }
    else {
      return NULL;
    }
  }

  BlockManager* getBlockManager(std::string acc_id) {
    if (acc_table.find(acc_id) != acc_table.end()) {
      return block_manager_table[acc_table[acc_id]->getType()].get();
    }
    else {
      return NULL;
    }
  }

  AccWorker getConfig(std::string acc_id) {
    // exception should be handled by previous steps
    return acc_config_table[acc_id];
  }

  void addShared(int64_t block_id, DataBlock_ptr block);

  DataBlock_ptr getShared(int64_t block_id);

  void removeShared(int64_t block_id);

private:
  QueueManager *queue_manager;
  Logger *logger;

  // map AccType to TaskEnv
  std::map<AccType, TaskEnv_ptr> env_table;

  // map acc_id to TaskEnv
  std::map<std::string, TaskEnv_ptr> acc_table;

  // map acc_id to AccWorker (acc configuration)
  std::map<std::string, AccWorker> acc_config_table;

  // map AccType to BlockManager
  std::map<AccType, BlockManager_ptr> block_manager_table;
};
}
#endif 
