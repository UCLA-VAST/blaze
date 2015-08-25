#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>

#include "blaze.h" 
using namespace blaze;

#include "NLBApp.h"

class NLB : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  NLB(): Task(1) {;}

  // overwrites the compute function
  virtual void compute() {

    // get input data length
    int data_length = getInputLength(0);

    if (data_length == 0 ||
        data_length % 8 != 0)
    {
      throw std::runtime_error("Invalid data length");
    }

    // get the pointer to input/output data
    int n_items = getInputNumItems(0);
    double* src_data = (double*)getInput(0);
    double* dst_data = (double*)getOutput(
                                0, data_length/n_items, n_items,
                                sizeof(double));

    printf("#items=%d, length=%d\n", n_items, data_length/n_items);

    if (!src_data || !dst_data) {
      throw std::runtime_error("Cannot get data pointers");
    }

    RuntimeClient  runtimeClient;
    if(!runtimeClient.isOK()){
      throw std::runtime_error("Runtime Failed to Start");
    }

    HelloCCINLBApp theApp(
        dst_data, src_data, 
        data_length*sizeof(double), &runtimeClient);

    // start real computation
    theApp.run();
  }
};

extern "C" Task* create() {
  return new NLB();
}

extern "C" void destroy(Task* p) {
  delete p;
}
