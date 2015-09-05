#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>

#include "blaze.h" 
#include "OpenCLEnv.h" 

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

    struct	timeval t1, t2, tr;

    try {
      // dynamically cast the TaskEnv to OpenCLEnv
      OpenCLEnv* ocl_env = (OpenCLEnv*)getEnv();

      // get input data length
      //int nsample = getInputNumItems(0);
      int data_length = getInputLength(0);
      int weight_length = getInputLength(1);

      //printf("data length = %d\n", data_length);
      //printf("weight length = %d\n", weight_length);

      // check input size
      if (data_length % (LABEL_SIZE+FEATURE_SIZE) != 0 || 
          data_length / (LABEL_SIZE+FEATURE_SIZE) == 0 ||
          weight_length != (LABEL_SIZE*(FEATURE_SIZE+1)))
      {
        fprintf(stderr, "Invalid input data dimensions:\n");
        fprintf(stderr, "data_length = %d\n", data_length);
        fprintf(stderr, "weight_length = %d\n", weight_length);
        throw std::runtime_error("Invalid input data dimensions");
      }

      // perform computation
      int nsample = data_length / 
        (LABEL_SIZE+FEATURE_SIZE);

      int L = LABEL_SIZE;
      int D = FEATURE_SIZE;

      int m = LABEL_SIZE;
      int n = FEATURE_SIZE;
      int inc = 1;
      float alpha = 1.0f;
      float beta = .0f;

      cl_context       context = ocl_env->getContext();
      cl_kernel        kernel  = ocl_env->getKernel();
      cl_command_queue command = ocl_env->getCmdQueue();

      int err;
      cl_event event;

      // get the pointer to input/output data
      cl_mem data      = *((cl_mem*)getInput(0));
      cl_mem weights   = *((cl_mem*)getInput(1));
      cl_mem gradients = *((cl_mem*)getOutput(
            0, weight_length, 1,
            sizeof(float)));

      if (!data || !weights || !gradients) {
        throw std::runtime_error("Buffer are not allocated");
      }

      // added part for debugging
      /*
      float* weight_scope = (float*)malloc(10*sizeof(float));
      float* gradient_scope = (float*)malloc(10*sizeof(float));

      err = clEnqueueReadBuffer(command, 
          weights, CL_TRUE, 0, sizeof(float) * 10, 
          weight_scope, 0, NULL, &event );  

      clWaitForEvents(1, &event);
      */

      // Set the arguments to our compute kernel
      err  = clSetKernelArg(kernel, 0, sizeof(int), &nsample);
      err |= clSetKernelArg(kernel, 1, sizeof(cl_mem), &weights);
      err |= clSetKernelArg(kernel, 2, sizeof(cl_mem), &data);
      err |= clSetKernelArg(kernel, 3, sizeof(cl_mem), &gradients);
      if (err != CL_SUCCESS) {
        throw std::runtime_error("Failed to set gradients!");
      }

      gettimeofday(&t1, NULL);

      // Execute the kernel over the entire range of our 1d input data set
      // using the maximum number of work group items for this device
      err = clEnqueueTask(command, kernel, 0, NULL, &event);
      clWaitForEvents(1, &event);

      if (err) {
        throw("Failed to execute kernel!");
      }

      /*
      err = clEnqueueReadBuffer(command, 
          gradients, CL_TRUE, 0, sizeof(float) * 10, 
          gradient_scope, 0, NULL, &event );  

      clWaitForEvents(1, &event);

      for (int i=0; i<10; i++) {
        printf("%f, %f\n", weight_scope[i], gradient_scope[i]);
      }
      */

      gettimeofday(&t2, NULL);
      timersub(&t1, &t2, &tr);
      fprintf(stdout, "FPGA execution takes %.4f ms\n", 
          fabs(tr.tv_sec*1000.0+(double)tr.tv_usec/1000.0));
    }
    catch (std::runtime_error &e) {
      throw e;
    }
  }
};

extern "C" Task* create() {
  return new Logistic();
}

extern "C" void destroy(Task* p) {
  delete p;
}
