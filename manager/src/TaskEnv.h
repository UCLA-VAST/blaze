#ifndef TASKENV_H
#define TASKENV_H

#include <boost/smart_ptr.hpp>

#include "proto/acc_conf.pb.h"

namespace acc_runtime {

class TaskEnv {

  public: 
    virtual void setup(AccConf &conf) {;}

    // TODO
    virtual void setupContext(AccConf &conf) {;}
    virtual void setupTask(AccConf &conf) {;}

};

typedef boost::shared_ptr<TaskEnv> TaskEnv_ptr;
const TaskEnv_ptr NULL_TASK_ENV;
}
#endif
