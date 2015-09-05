#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>
#include <limits>

//#define USEMKL

#ifdef USEMKL
#include <mkl.h>
#endif

#include "blaze.h" 

using namespace blaze;

class LogisticGradientWithLoss : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  LogisticGradientWithLoss(): Task(2) {;}

  // overwrites the compute function
  // Input data:
  // - data: layout as num_samples x [double label, double[] feature]
  // - weight: (num_labels-1) x feature_length
  // Output data:
  // - gradient plus loss: [double[] gradient, double loss]
  virtual void compute() {

    // get input data length
    int data_length = getInputLength(0);
    int num_samples = getInputNumItems(0);
    int weight_length = getInputLength(1);
    int feature_length = data_length / num_samples - 1;
    int num_labels = weight_length / feature_length + 1;

    // check input size
    if (weight_length % feature_length != 0 || 
        num_labels < 2)
    {
      fprintf(stderr, "num_samples=%d, feature_length=%d, weight_length=%d\n", num_samples, feature_length, weight_length);
      throw std::runtime_error("Invalid input data dimensions");
    }

    // get the pointer to input/output data
    double * data     = (double*)getInput(0);
    double * weights  = (double*)getInput(1);
    double * output   = (double*)getOutput(0, weight_length+1, 1, sizeof(double));

    if (!data || !weights || !output) {
      throw std::runtime_error("Cannot get data pointers");
    }

    // perform computation

    int L = num_labels;
    int D = feature_length;

    memset(output, 0, sizeof(double)*(weight_length+1));

#ifdef USEMKL
		int m = L;
		int n = D;
		int inc = 1;
		float alpha = 1.0f;
		float beta = .0f;

    for(int k = 0; k < nsample; k++ ) {

      cblas_sgemv(
          CblasRowMajor, CblasNoTrans, 
          m, n, alpha, 
          weights, n, 
          data+k*(D+1)+1, 
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
    // TODO: is it really L-1 not L?
    double* margins = new double[L-1];

    for(int k = 0; k < num_samples; k++ ) {
      
      double marginY = 0.0;
      double maxMargin = -std::numeric_limits<double>::infinity();
      int    maxMarginIndex = 0;

      double  label = data[k*(D+1)];
      double* feature = data + k*(D+1) + 1;

      for (int i=0; i<L-1; i++) {
        double margin = 0.0;
        for(int j = 0; j < D; j+=1 ) {
          margin += weights[i*D+j] * feature[j];
        }
        if (i == (int)label - 1) {
          marginY = margin;
        }
        if (margin > maxMargin) {
          maxMargin = margin;
          maxMarginIndex = i;
        }
        margins[i] = margin;
      }

      double sum = 0.0;
      for (int i=0; i<L-1; i++) {
        if (maxMargin > 0) {
          margins[i] -= maxMargin;
          if (i == maxMarginIndex) {
            sum += exp(-maxMargin);
          }
          else {
            sum += exp(margins[i]);
          }
        } 
        else {
          sum += exp(margins[i]);
        }
      }

      // update gradient
      for(int i = 0; i < L-1; i++ ) {
        double multiplier = exp(margins[i]) / (sum+1.0);
        if (label != 0.0 && label == i+1) {
          multiplier -= 1.0;
        }
        for(int j = 0; j < D; j++ ) {
          output[i*D+j] +=  multiplier*feature[j];
        }
      }

      // compute loss
      double loss = log(sum+1); // math.logip(sum)
      if (label > 0.0) {
        loss -= marginY;
      }
      if (maxMargin > 0) {
        loss += maxMargin;
      }
      output[weight_length] += loss;
    }
#endif
  }
};

extern "C" Task* create() {
  return new LogisticGradientWithLoss();
}

extern "C" void destroy(Task* p) {
  delete p;
}
