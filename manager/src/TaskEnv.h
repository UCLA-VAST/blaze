#ifndef TASKENV_H
#define TASKENV_H

#include <boost/smart_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

namespace blaze {

class Platform;

class TaskEnv 
  : public boost::basic_lockable_adapter<boost::mutex> 
{
protected: 
  virtual void setPlatform(Platform* platform) {;}
};

typedef boost::shared_ptr<TaskEnv> TaskEnv_ptr;
const TaskEnv_ptr NULL_TASK_ENV;
}
#endif
