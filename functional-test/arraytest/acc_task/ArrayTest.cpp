#include <stdio.h>
#include <stdlib.h>

#include "blaze.h" 

using namespace blaze;

class ArrayTest : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  ArrayTest(): Task(2) {;}

  // overwrites the compute function
  virtual void compute() {

    // get input data length
    int data_length = getInputLength(0);
		int num_samples = getInputNumItems(0);
		int item_length = data_length / num_samples;
		int weight_length = getInputLength(1);

    // get the pointer to input/output data
    double* a = (double*)getInput(0);
		double* val = (double*)getInput(1);
    double* b = (double*)getOutput(0, item_length, num_samples, sizeof(double));

    // perform computation
    double val_sum = 0.0;
    for (int k = 0; k < weight_length; k++) {
      val_sum += val[k];
    }

    for (int i = 0; i < num_samples; i++) {
			for (int j = 0; j < item_length; j++) {
        b[i * item_length + j] = a[i * item_length +j] + val_sum;
			}
    }
    // if there is any error, throw exceptions
  }
};

extern "C" Task* create() {
  return new ArrayTest();
}

extern "C" void destroy(Task* p) {
  delete p;
}
