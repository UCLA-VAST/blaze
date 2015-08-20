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

#ifndef TASKENV_H
#define TASKENV_H

#include <boost/smart_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include "proto/acc_conf.pb.h"
#include "Block.h"

namespace blaze {

class TaskEnv 
  : public boost::basic_lockable_adapter<boost::mutex> 
{
public: 
  TaskEnv(AccType _type): type(_type) {;}

  virtual void setup(AccWorker &conf) {;}

  // TODO
  virtual void setupContext(AccWorker &conf) {;}
  virtual void setupTask(AccWorker &conf) {;}

  AccType getType() { return type; }

protected:
  AccType type;

};

typedef boost::shared_ptr<TaskEnv> TaskEnv_ptr;
const TaskEnv_ptr NULL_TASK_ENV;
}
#endif
