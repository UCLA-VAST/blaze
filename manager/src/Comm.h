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

#ifndef COMM_H
#define COMM_H

#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include "task.pb.h"
#include "acc_conf.pb.h"

#include "Context.h"
#include "BlockManager.h"
#include "QueueManager.h"
#include "TaskManager.h"
#include "Logger.h"

using namespace boost::asio;

typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

namespace blaze {

class AccReject : public std::logic_error {
public:
  explicit AccReject(const std::string& what_arg):
    std::logic_error(what_arg) {;}
};

class AccFailure : public std::logic_error {
public:
  explicit AccFailure(const std::string& what_arg):
    std::logic_error(what_arg) {;}
};

/*
 * Communicator design for Acc_Manager
 */
class Comm 
: public boost::basic_lockable_adapter<boost::mutex>
{
public:
  Comm(
      Context* _context,
      QueueManager* _queue_manager,
      Logger* _logger,
      std::string address = "127.0.0.1",
      int ip_port = 1027
    ):
    ip_address(address), 
    srv_port(ip_port), 
    context(_context),
    queue_manager(_queue_manager),
    logger(_logger)
  { 
    // asynchronously start listening for new connections
    boost::thread t(boost::bind(&Comm::listen, this));
  }

  void addTask(std::string id);
  void removeTask(std::string id);
  
private:
  void recv(TaskMsg&, ip::tcp::iostream&);
  void send(TaskMsg&, ip::tcp::iostream&);

  // processing messages
  void process(socket_ptr);

  void listen();

  int srv_port;
  std::string ip_address;

  std::map<std::string, int> num_tasks;

  // reference to context
  Context *context;

  // reference to queue manager
  QueueManager *queue_manager;

  Logger *logger;
};
}

#endif
