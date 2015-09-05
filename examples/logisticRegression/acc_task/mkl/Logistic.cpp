#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>

#define USEMKL

#ifdef USEMKL
#include <mkl.h>
#endif

#include "blaze.h" 

using namespace blaze;

#define LABEL_SIZE		10
#define FEATURE_SIZE	784

class Logistic : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  Logistic(): Task(2) {;}

  // overwrites the readLine runction
  virtual char* readLine(
      std::string line, 
      size_t &num_elements, 
      size_t &num_bytes) 
  {

    // allocate return buffer here, the consumer 
    // will be in charge of freeing the memory
    float* result = new float[LABEL_SIZE + FEATURE_SIZE];

    num_bytes = (LABEL_SIZE+FEATURE_SIZE)*sizeof(float);
    num_elements = (LABEL_SIZE+FEATURE_SIZE);

    std::vector<float>* v = new std::vector<float>;

    std::istringstream iss(line);

    std::copy(std::istream_iterator<float>(iss),
        std::istream_iterator<float>(),
        std::back_inserter(*v));

    return (char*)v;
  }

  // overwrites the compute function
  virtual void compute() {

    // get input data length
    int data_length = getInputLength(0);
    int weight_length = getInputLength(1);

    // check input size
    if (data_length % (LABEL_SIZE+FEATURE_SIZE) != 0 || 
        data_length / (LABEL_SIZE+FEATURE_SIZE) == 0 ||
        weight_length != (LABEL_SIZE*(FEATURE_SIZE+1)))
    {
			fprintf(stderr, "Invalid input data dimensions\n");
      throw std::runtime_error("Invalid input data dimensions");
    }

    // get the pointer to input/output data
    float* data     = (float*)getInput(0);
    float* weights  = (float*)getInput(1);
    float* gradient = (float*)getOutput(
                                0, weight_length, 1,
                                sizeof(float));

    if (!data || !weights || !gradient) {
			fprintf(stderr, "Cannot get data pointers\n");
      throw std::runtime_error("Cannot get data pointers");
    }

    // perform computation
    int nsample = data_length / 
          (LABEL_SIZE+FEATURE_SIZE);

    //printf("processing %d data points\n", nsample);

    int L = LABEL_SIZE;
    int D = FEATURE_SIZE;

		int m = LABEL_SIZE;
		int n = FEATURE_SIZE;
		int inc = 1;
		float alpha = 1.0f;
		float beta = .0f;

    memset(gradient, 0, sizeof(float)*LABEL_SIZE*FEATURE_SIZE);

#ifdef USEMKL
	  float label[L];
    for(int k = 0; k < nsample; k++ ) {

      cblas_sgemv(
          CblasRowMajor, CblasNoTrans, 
          m, n, alpha, 
          weights, n, 
          data+k*(D+L)+L, 
          inc, beta, 
          label, inc);

      for (int i=0; i<L; i++) {
        float coeff = (1. / 
            (1. + exp(-data[k*(D+L)+i]*label[i] )) 
            - 1.)* data[k*(D+L)+i];

        cblas_saxpy(
            n, coeff, 
            data+k*(D+L)+L, inc, 
            gradient+i*(D+1), inc);
      }
    }
#else
    float label[LABEL_SIZE];
    for(int k = 0; k < nsample; k++ ) {
      for(int i = 0; i < L; i+=1 ) {
        float dot = 0.;
        for(int j = 0; j < D; j+=1 ) {
          dot += weights[i*D+j+0]*data[k*(D+L)+j+0+L];
        }
        float coeff = (1. / (1. + exp(-data[k*(D+L)+i]*dot )) - 1.)*data[k*(D+L)+i];
        for(int j = 0; j < D; j+=1 ) {
          gradient[i*D+j+0] +=  coeff*data[k*(D+L)+j+0+L];
        }
      }
    }
#endif
  }
};

extern "C" Task* create() {
  return new Logistic();
}

extern "C" void destroy(Task* p) {
  delete p;
}
