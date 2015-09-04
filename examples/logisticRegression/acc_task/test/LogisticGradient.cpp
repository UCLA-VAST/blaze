#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string>
#include <fstream>

#include "BlazeTest.h"

using namespace blaze;

int main(int argc, char** argv) {

  if (argc < 2) {
    printf("USAGE: %s <conf_path>\n", argv[0]);
    return -1;
  }

  try {
    BlazeTest<double, double> test(argv[1], 1e-5);

    // prepare data
    int feature_size = 784;
    int num_labels = 10;
    int num_samples = 1024;
    int data_size = (feature_size+1) * num_samples;
    int weight_size = (num_labels-1) * feature_size;

    // input
    double* data_samples = new double[data_size];
    double* weights = new double[weight_size];

    for (int i=0; i<num_samples; i++) {
      data_samples[i*(feature_size+1)] = rand()%num_labels; 
      for (int j=0; j<feature_size; j++) {
        data_samples[i*(feature_size+1)+1+j] = (double)rand()/RAND_MAX;
      }
    }
    for (int i=0; i<weight_size; i++) {
      weights[i] = (double)rand()/RAND_MAX;
    }

    // setup input data for tested tasks
    test.setInput(0, data_samples, num_samples, data_size);
    test.setInput(1, weights, 1, weight_size);

    // run test
    test.run();

    delete data_samples;
    delete weights;
  }
  catch (std::runtime_error &e) {
    printf("%s\n", e.what());
    return -1;
  }

  return 0;
}
