#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>

#include "blaze/Task.h" 
using namespace blaze;

class LB : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  LB(): Task(1) {;}

  // overwrites the compute function
  virtual void compute() {

    // get input data length
    int data_length = getInputLength(0);

    if (data_length == 0) {
      throw std::runtime_error("Invalid data length");
    }

    // get the pointer to input/output data
    double* src_data = (double*)getInput(0);
    double* dst_data = (double*)getOutput(0, data_length, 1, sizeof(double));

    if (!src_data || !dst_data) {
      throw std::runtime_error("Cannot get data pointers");
    }
    memcpy(dst_data, src_data, data_length*sizeof(double)); 
  }
};

extern "C" Task* create() {
  return new LB();
}

extern "C" void destroy(Task* p) {
  delete p;
}
