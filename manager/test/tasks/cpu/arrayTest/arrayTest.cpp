#include <stdio.h>
#include <stdlib.h>

#include "blaze/Task.h" 

using namespace blaze;

class ArrayTest : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  ArrayTest(): Task(2) {;}

  // overwrites the compute function
  virtual void compute() {

    // get input data dimensions
    int total_length  = getInputLength(0);
    int num_vectors   = getInputNumItems(0);
    int vector_length = total_length / num_vectors;

    // get the pointer to input/output data
    double* a = (double*)getInput(0);
    double* b = (double*)getInput(1);
    double* c = (double*)getOutput(0, 
        vector_length, num_vectors, sizeof(double));

    // perform computation
    // c[i] = a[i] + b
    for (int i=0; i<num_vectors; i++) {
      for (int j=0; j<vector_length; j++) {
        c[i * vector_length + j] = a[i * vector_length + j] + b[j];
      }
    }
  }
};

// define the constructor and destructor for dlopen()
extern "C" Task* create() {
  return new ArrayTest();
}

extern "C" void destroy(Task* p) {
  delete p;
}
