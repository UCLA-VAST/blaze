#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
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

class LogisticGradientWithLoss_mkl : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  LogisticGradientWithLoss_mkl(): Task(2) {;}

  // overwrites the compute function
  // Input data:
  // - data: layout as num_samples x [double label, double[] feature]
  // - weight: (num_labels-1) x feature_length
  // Output data:
  // - gradient plus loss: [double[] gradient, double loss]
  virtual void compute() {

    struct	timeval t1, t2, tr;

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

    double* margins = new double[L-1];

    gettimeofday(&t1, NULL);
    //for (int k = 0; k < 100; k++ ) {
    for(int k = 0; k < num_samples; k++ ) {
      
      double marginY = 0.0;
      double maxMargin = -std::numeric_limits<double>::infinity();
      int    maxMarginIndex = 0;

      double  label = data[k*(D+1)];
      double* feature = data + k*(D+1) + 1;

      for (int i=0; i<L-1; i++) {
        margins[i] = 0.0;
      }

      // gemv
#ifdef USEMKL
      int inc = 1;
      int m = L-1;
      int n = D;
      double alpha = 1.0;
      double beta = 0.0;

      cblas_dgemv(
          CblasRowMajor, CblasNoTrans, 
          m, n, alpha, 
          weights, n, 
          feature, 
          inc, beta, 
          margins, inc);
#else
      for (int i=0; i<L-1; i++) {
        for(int j = 0; j < D; j+=1 ) {
          margins[i] += weights[i*D+j] * feature[j];
        }
      }
#endif
      for (int i=0; i<L-1; i++) {
        if (i == (int)label - 1) {
          marginY = margins[i];
        }
        if (margins[i] > maxMargin) {
          maxMargin = margins[i];
          maxMarginIndex = i;
        }
      }

      for (int i=0; i<L-1; i++) {
        if (maxMargin > 0) {
          margins[i] -= maxMargin;
        }
        margins[i] = exp(margins[i]);
      }

      double sum = 0.0;
      for (int i=0; i<L-1; i++) {
        if (i == maxMarginIndex && maxMargin > 0) {
          sum += exp(-maxMargin);
        }
        else {
          sum += margins[i];
        }
      }

      // update gradient
      for(int i = 0; i < L-1; i++ ) {
        double multiplier =margins[i] / (sum+1.0);
        if (label != 0.0 && label == i+1) {
          multiplier -= 1.0;
        }

        // axpy
#ifdef USEMKL
        cblas_daxpy(
            n, multiplier, 
            feature, inc, 
            output+i*D, inc);
#else
        for(int j = 0; j < D; j++ ) {
          output[i*D+j] += multiplier*feature[j];
        }
#endif
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

    gettimeofday(&t2, NULL);
    timersub(&t1, &t2, &tr);
    printf("Task execution takes %.4f ms\n", 
        fabs(tr.tv_sec*1000.0+(double)tr.tv_usec/1000.0));

    delete [] margins;
  }
};

extern "C" Task* create() {
  return new LogisticGradientWithLoss_mkl();
}

extern "C" void destroy(Task* p) {
  delete p;
}
