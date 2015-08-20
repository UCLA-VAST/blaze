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

#define SIZE	768

class LoopBack : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  LoopBack(TaskEnv* env): Task(env, 1) {;}

  // overwrites the readLine runction
  virtual char* readLine(
      std::string line, 
      size_t &num_elements, 
      size_t &num_bytes) 
  {

    // allocate return buffer here, the consumer 
    // will be in charge of freeing the memory
    float* result = new float[SIZE];

    num_bytes = (SIZE)*sizeof(float);
    num_elements = SIZE;

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

    // get the pointer to input/output data
    float* data = (float*)getInput(0);
    float* out	= (float*)getOutput(
                                0, data_length, 1,
                                sizeof(float));

    if (!data || !out) {
			fprintf(stderr, "Cannot get data\n");
      throw std::runtime_error("Cannot get data");
    }

    memcpy(out, data, sizeof(float)*SIZE);

  }
};

extern "C" Task* create(TaskEnv* env) {
  return new LoopBack(env);
}

extern "C" void destroy(Task* p) {
  delete p;
}
