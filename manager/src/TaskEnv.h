#ifndef TASKENV_H
#define TASKENV_H

#include <boost/smart_ptr.hpp>

#include "proto/acc_conf.pb.h"
#include "Block.h"

namespace acc_runtime {

class TaskEnv {

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
