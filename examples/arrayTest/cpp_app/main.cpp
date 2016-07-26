#define LOG_HEADER "main"
#include <glog/logging.h>

#include  "blaze/Client.h"

using namespace blaze;

class ArrayTestClient: public Client {
public:
  ArrayTestClient(): Client("ArrayTest", 2, 1) {;}

  void compute() {
    int num_samples     = getInputNumItems(0);
    int feature_size    = getInputLength(1);

    double* data_ptr    = (double*)getInputPtr(0);
    double* weight_ptr  = (double*)getInputPtr(1);
    double* output_ptr  = (double*)createOutput(0, 
                              num_samples, feature_size, sizeof(double));

    for (int i = 0; i < num_samples; i++) {
      for (int j = 0; j < feature_size; j++) {
        output_ptr[i * feature_size + j] = 
          data_ptr[i * feature_size +j] + 
          weight_ptr[j];
      }
    }
  }
};

int main(int argc, char** argv) {

  // GLOG configuration
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  FLAGS_v = 2;

  if (argc < 2) {
    printf("USAGE: %s <num_samples>\n", argv[0]);
    return -1;
  }

  int num_samples = atoi(argv[1]);
  int feature_size = 1024;

  if (argc > 3) {
    feature_size = atoi(argv[2]);
  }
  int data_size = num_samples*feature_size;

  try {
    ArrayTestClient client;

    double* data_ptr    = (double*)client.createInput(0, num_samples, feature_size, sizeof(double), BLAZE_INPUT);
    double* weight_ptr  = (double*)client.createInput(1, feature_size, 1, sizeof(double), BLAZE_INPUT);

    double* output_base = new double[num_samples*feature_size];

    // setup input with random data
    for (int i=0; i<num_samples; i++) {
      for (int j=0; j<feature_size; j++) {
        data_ptr[i*feature_size+j] = (double)rand()/RAND_MAX;
      }
    }
    for (int i=0; i<feature_size; i++) {
      weight_ptr[i] = (double)rand()/RAND_MAX;
    }

    // start computation
    client.start();

    // compute baseline results
    for (int i = 0; i < num_samples; i++) {
			for (int j = 0; j < feature_size; j++) {
        output_base[i * feature_size + j] = 
                  data_ptr[i * feature_size +j] + 
                  weight_ptr[j];
			}
    }
    double* output_ptr = (double*)client.getOutputPtr(0);

    // compare results
    double diff_total = 0.0;
    double diff_ratio = 0.0;
    double max_diff = 0.0;
    for (int k=0; k<data_size; k++) {
      double diff = std::abs(output_base[k] - output_ptr[k]); 
      if (diff > max_diff) {
        max_diff = diff;
      }

      diff_total += diff;
      if (output_base[k]!=0) {
        diff_ratio += diff / std::abs(output_base[k]);
      }

      if (diff / std::abs(output_base[k]) > 0.05 && 
          k<10) 
      {
        printf("%d: %f|%f, ratio=%f\n", 
            k,
            output_base[k], 
            output_ptr[k],
            diff/std::abs(output_base[k]));
      }
    }
    if (diff_total < 1e-6) {
      printf("Result correct\n");
    }
    else {
      printf("Result incorrect\n");
    }
  }
  catch (std::exception &e) {
    printf("%s\n", e.what());
    return -1;
  }

  return 0;
}
