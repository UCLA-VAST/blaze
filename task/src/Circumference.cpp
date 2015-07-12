#include <stdio.h>
#include <stdlib.h>

#include "acc_runtime.h" 

using namespace acc_runtime;

class Circumference : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  Circumference(): Task(2) {;}

  // overwrites the compute function
  virtual void compute() {
     
    int data_length = input_blocks[0]->getLength();
    int data_size = input_blocks[0]->getSize();

    //DataBlock_ptr input_block = input[0];
    DataBlock_ptr out_block = createOutputBlock(data_length, data_size);

    double* a = (double*)(input_blocks[0]->getData());
    double* pi = (double*)(input_blocks[1]->getData());
    double* b = (double*)(out_block->getData());

    for (int i = 0; i < data_length; i++) {
      b[i] = a[i] * pi[0]; // FIXME: How to use broadcast variable?
    }

    // if there is any error, throw exceptions
  }
};

extern "C" Task* create() {
  return new Circumference;
}

extern "C" void destroy(Task* p) {
  delete p;
}
