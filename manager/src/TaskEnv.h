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
