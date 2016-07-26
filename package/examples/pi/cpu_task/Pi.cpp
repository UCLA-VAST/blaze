#include <cstdint>
#include <stdio.h>
#include <stdlib.h>

#include "blaze/Task.h" 

using namespace blaze;

class Pi : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  Pi(): Task(1) {;}

  // overwrites the compute function
  virtual void compute() {

    // get input data dimensions
    int num_data = getInputNumItems(0);

    // get the pointer to input/output data
    double* input = (double*)getInput(0);
    uint32_t* output = (uint32_t*)getOutput(
        0, 1, 2, sizeof(uint32_t));

    uint32_t inside_count = 0;
    uint32_t outside_count = 0;

    // perform computation
    for (int i=0; i<num_data; i++) {
      double x = input[i*2+0];
      double y = input[i*2+1];
      if (x*x+y*y < 1.0) {
        inside_count++;
      }
      else {
        outside_count++;
      }
    }
    output[0] = inside_count;
    output[1] = outside_count;
  }
};

// define the constructor and destructor for dlopen()
extern "C" Task* create() {
  return new Pi();
}

extern "C" void destroy(Task* p) {
  delete p;
}
