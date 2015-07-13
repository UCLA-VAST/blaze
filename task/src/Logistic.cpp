#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdexcept>

#include <mkl.h>
#include "acc_runtime.h" 

using namespace acc_runtime;

#define LABEL_SIZE		10
#define FEATURE_SIZE	784

class Logistic : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  Logistic(): Task(2) {;}

  // overwrites the compute function
  virtual void compute() {

    // get input data length
    int data_length = getInputLength(0);
    int weight_length = getInputLength(1);

    // check input size
    if (data_length % (LABEL_SIZE+FEATURE_SIZE) != 0 || 
        data_length / (LABEL_SIZE+FEATURE_SIZE) == 0 ||
        weight_length != LABEL_SIZE*FEATURE_SIZE)
    {
      throw std::runtime_error("Invalid input data dimensions");
      return;
    }

    // get the pointer to input/output data
    float* data     = (float*)getInput(0);
    float* weights  = (float*)getInput(1);
    float* gradient = (float*)getOutput(
                                0, 
                                weight_length, 
                                sizeof(float));

	  float label[LABEL_SIZE];

    // perform computation
    int nsample = data_length / (LABEL_SIZE+FEATURE_SIZE);

		int m = LABEL_SIZE;
		int n = FEATURE_SIZE;
		int inc = 1;
		float alpha = 1.0f;
		float beta = .0f;

    for(int k = 0; k < nsample; k++ ) {

      cblas_sgemv(
          CblasRowMajor, CblasNoTrans, 
          m, n, alpha, 
          weights, n, 
          data+k*(FEATURE_SIZE+LABEL_SIZE)+LABEL_SIZE, 
          inc, beta, 
          label, inc);

      for (int i=0; i<LABEL_SIZE; i++) {
        float coeff = (
            1. / 
            (1. + exp(
                      -data[k*(FEATURE_SIZE+LABEL_SIZE)+i]*
                      label[i])
            ) - 1.)* data[k*(FEATURE_SIZE+LABEL_SIZE)+i];

        cblas_saxpy(
            n, coeff, 
            data+k*(FEATURE_SIZE+LABEL_SIZE)+LABEL_SIZE, 
            inc, 
            gradient+i*FEATURE_SIZE, inc);
      }
    }

  }
};

extern "C" Task* create() {
  return new Logistic;
}

extern "C" void destroy(Task* p) {
  delete p;
}
