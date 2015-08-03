#ifndef CONTEXT_H
#define CONTEXT_H

#include <string>
#include <vector>
#include <map>

#include <boost/smart_ptr.hpp>

#include "acc_conf.pb.h"

#include "Logger.h"
#include "TaskEnv.h"
#include "OpenCLEnv.h"

namespace acc_runtime {

class Context {

public:
  Context(Logger* _logger): logger(_logger) {;}

  void createEnv(AccType type) {
    if (env_table.find(type) != env_table.end()) {
      return;  
    }
    switch (type) {
      case AccType::OpenCL :
        try {

          TaskEnv_ptr env(new OpenCLEnv());

          env_table.insert(std::make_pair(type, env));

          logger->logInfo(
              std::string("Context::")+
              std::string(__func__)+
              std::string("(): created OpenCLEnv()"));

        } catch (std::runtime_error &e) {
          logger->logErr(
              std::string("Context::")+
              std::string(__func__)+
              std::string("(): cannot create OpenCL env"));
        }
        break;
      default :
        TaskEnv_ptr env(new TaskEnv());
        env_table.insert(std::make_pair(type, env));
        break;
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

private:
  Logger *logger;
  std::map<AccType, TaskEnv_ptr> env_table;
};
}
#endif 
