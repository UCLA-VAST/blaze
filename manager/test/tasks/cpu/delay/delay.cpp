#include <string>
#include <vector>
#include <algorithm>
#include <sstream>

#include "blaze/Task.h" 
using namespace blaze;

class Delay : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  Delay(): Task(1) {;}

  // overwrites the compute function
  virtual void compute() {
    sleep(1);
  }
};

extern "C" Task* create() {
  return new Delay();
}

extern "C" void destroy(Task* p) {
  delete p;
}
