#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>
#include <limits>

#define DUMP

#include "blaze.h" 

using namespace blaze;

class HingeGradient : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  HingeGradient(): Task(2) {;}

  // Input data:
  // - data: layout as num_samples x [double label, double[] feature]
  // - weight: 1 x feature.length
  // Output data:
  // - (gradient, loss, count): [double[] gradient, double loss, double count]
  virtual void compute() {

    // get input data length
    int data_length = getInputLength(0);
    int num_samples = getInputNumItems(0);
    int weight_length = getInputLength(1);
    int feature_length = data_length / num_samples - 1;
    int num_labels = weight_length / feature_length + 1;

    // check input size
    if (weight_length != feature_length || 
        num_labels != 2)
    {
      fprintf(stderr, "num_samples=%d, feature_length=%d, weight_length=%d\n", num_samples, feature_length, weight_length);
      throw std::runtime_error("Invalid input data dimensions");
    }

    // get the pointer to input/output data
    double * data     = (double*)getInput(0);
    double * weights  = (double*)getInput(1);
    double * output   = (double*)getOutput(0, weight_length+2, 1, sizeof(double));

    if (!data || !weights || !output) {
      throw std::runtime_error("Cannot get data pointers");
    }

    // perform computation
    memset(output, 0, sizeof(double)*(weight_length+2));

    for (int k = 0; k < num_samples; k++ ) {

      int element_offset = feature_length + 1;
      
      double  label = data[k*element_offset];
      double* feature = data + k*element_offset + 1;

      double labelScaled = 2*label - 1.0;
      double dotProduct  = 0.0;

      for(int j = 0; j < feature_length; j++ ) {
        dotProduct += weights[j] * feature[j];
      }

      if (1.0 > labelScaled * dotProduct) {
        for (int j=0; j<weight_length; j++) {
          output[j] += -labelScaled * feature[j];
        }
        output[weight_length] += 1.0 - labelScaled * dotProduct;
      }
    }
    output[weight_length+1] = (double)num_samples;
  }
};

extern "C" Task* create() {
  return new HingeGradient();
}

extern "C" void destroy(Task* p) {
  delete p;
}
