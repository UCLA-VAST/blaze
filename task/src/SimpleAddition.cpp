#include <stdio.h>
#include <stdlib.h>

#include "acc_runtime.h" 

using namespace acc_runtime;

class SimpleAddition : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  SimpleAddition(): Task(1) {;}

  // overwrites the compute function
  virtual void compute() {

    // get input data length
    int data_length = getInputLength(0);

    // get the pointer to input/output data
    double* a = (double*)getInput(0);
    double* b = (double*)getOutput(0, data_length, sizeof(double));

    // perform computation
    for (int i=0; i<data_length; i++) {
      b[i] = a[i] + 1.0f;
    }

    // if there is any error, throw exceptions
  }
};

extern "C" Task* create() {
  return new SimpleAddition;
}

extern "C" void destroy(Task* p) {
  delete p;
}
