
#include  "BlazeClient.h"

#define LABEL_SIZE		10
#define FEATURE_SIZE	784

using namespace blaze;

int main(int argc, char** argv) {

  if (argc < 2) {
    printf("USAGE: %s <num_samples>\n", argv[0]);
    return -1;
  }

  int num_samples = atoi(argv[1]);
  int weight_size = 4;
  int feature_size = 784;

  if (argc > 3) {
    feature_size = atoi(argv[2]);
  }

  try {
    BlazeClient client("arraytest", "C++");

    double* data_ptr    = (double*)client.alloc(num_samples, feature_size, feature_size*sizeof(double), BLAZE_INPUT_CACHED);
    double* weight_ptr  = (double*)client.alloc(weight_size, 1, sizeof(double), BLAZE_INPUT);
    double* output_ptr  = (double*)client.alloc(num_samples, feature_size, feature_size*sizeof(double), BLAZE_OUTPUT);

    double* output_base = new double[num_samples*feature_size];

    // setup input with random data
    for (int i=0; i<num_samples; i++) {
      for (int j=0; j<feature_size; j++) {
        data_ptr[i*feature_size+j] = (double)rand()/RAND_MAX;
      }
    }
    for (int i=0; i<weight_size; i++) {
      weight_ptr[i] = (double)rand()/RAND_MAX;
    }

    // start computation
    client.start();

    // compute baseline results
    double val_sum = 0.0;
    for (int k = 0; k < weight_size; k++) {
      val_sum += weight_ptr[k];
    }

    for (int i = 0; i < num_samples; i++) {
			for (int j = 0; j < feature_size; j++) {
        output_base[i * feature_size + j] = data_ptr[i * feature_size +j] + val_sum;
			}
    }

    // compare results
    double diff_total = 0.0;
    double diff_ratio = 0.0;
    double max_diff = 0.0;
    for (int k=0; k<weight_size+1; k++) {
      double diff = std::abs(output_base[k] - output_ptr[k]); 
      if (diff > max_diff) {
        max_diff = diff;
      }

      diff_total += diff;
      if (output_base[k]!=0) {
        diff_ratio += diff / std::abs(output_base[k]);
      }

      if (diff / std::abs(output_base[k]) > 0.05) {
        printf("%d: %f|%f, ratio=%f\n", 
            k,
            output_base[k], 
            output_ptr[k],
            diff/std::abs(output_base[k]));
      }
    }
    printf("diff: %f max, %f/point, %f%/point\n",
        max_diff,
        diff_total/(weight_size+1),
        diff_ratio/(weight_size+1)*100.0);
  }
  catch (std::exception &e) {
    printf("%s\n", e.what());
    return -1;
  }

  return 0;
}
