
#include  "BlazeClient.h"

#define LABEL_SIZE		10
#define FEATURE_SIZE	784

using namespace blaze;

int main(int argc, char** argv) {
 
  float learning_rate = 0.13;
  int L = LABEL_SIZE;
  int D = FEATURE_SIZE;
  int n = 60000;
  int m = 10000;

  float* test_data = (float*)malloc(m*(D+L)*sizeof(float));
  FILE* pFile = fopen("/curr/bjxiao/projects/logistic/data/test_data.bin", "r");
  fread(test_data,sizeof(float),m*(D+L),pFile);
  fclose(pFile);

  pFile = fopen("/curr/bjxiao/projects/logistic/data/train_data.bin", "r");

  try {
    BlazeClient client("Logistic");

    float* data_ptr     = (float*)client.alloc(sizeof(float), n*(L+D), BLAZE_INPUT_CACHED);
    float* weight_ptr   = (float*)client.alloc(sizeof(float), L*(D+1), BLAZE_INPUT);
    float* gradient_ptr = (float*)client.alloc(sizeof(float), L*(D+1), BLAZE_OUTPUT);

    // read from file
    fread(data_ptr, sizeof(float), n*(D+L), pFile);

    // set weights to zero
    memset(weight_ptr, 0, L*(D+1)*sizeof(float));

    // start computation
    for (int iter=0; iter<20; iter++) {
      client.start();

      for (int i = 0; i < L; i++) {
        for (int j = 0; j < D+1; j++) {
          weight_ptr[i*(D+1)+j] -= learning_rate * gradient_ptr[i*(D+1)+j] / n;
        }
      }

      /*
      int errors = 0;
      for(int i = 0; i < m; i++)
      {
        float max_possibility = -10000;
        int likely_class = 0;
        for(int j = 0; j < L; j++)
        {
          float dot = weight_ptr[j*(D+1)];
          for(int k=0; k < D; k++)
          {
            dot += weight_ptr[j*(D+1)+k+1] * test_data[i*(D+L)+k+L];
          }
          if(dot > max_possibility)
          {
            max_possibility = dot;
            likely_class = j;
          }
        }
        if( test_data[i*(D+L)+likely_class] < 0.5 )
        {
          errors++;
        }
      }
      printf("error rate: %f\%\n", ((float)errors)/ m * 100.);
      */
    }
    fclose(pFile);
  }
  catch (std::exception &e) {
    printf("%s\n", e.what());
    return -1;
  }

  return 0;
}
