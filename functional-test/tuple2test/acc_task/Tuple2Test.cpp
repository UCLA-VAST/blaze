#include <stdio.h>
#include <stdlib.h>

#include "blaze.h" 

using namespace blaze;

class Tuple2Test : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  Tuple2Test(): Task(2) {;}

  // overwrites the compute function
  virtual void compute() {

    // get input data length
    int data_length = getInputLength(0);

    // get the pointer to input/output data
    int* a = (int*)getInput(0);
		int* b = (int*)getInput(1);
    int* c = (int*)getOutput(0, 1, data_length, sizeof(int));

    // perform computation
    for (int i=0; i<data_length; i++) {
      c[i] = a[i] + b[i];
    }

    // if there is any error, throw exceptions
  }
};

extern "C" Task* create() {
  return new Tuple2Test();
}

extern "C" void destroy(Task* p) {
  delete p;
}
