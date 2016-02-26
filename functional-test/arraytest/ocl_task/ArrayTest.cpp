#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <sstream>

#include "Task.h" 
#include "OpenCLEnv.h" 

using namespace blaze;

class ArrayTest : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  ArrayTest(): Task(2) {;}

  // overwrites the compute function
  // Input data:
  // - data: layout as num_samples x [double label, double[] feature]
  // - weight: (num_labels-1) x feature_length
  // Output data:
  // - gradient plus loss: [double[] gradient, double loss]
  virtual void compute() {

    struct	timeval t1, t2, tr;

    try {
      // dynamically cast the TaskEnv to OpenCLEnv
      OpenCLTaskEnv* ocl_env = (OpenCLTaskEnv*)getEnv();

      // get input data length
      int data_length = getInputLength(0);
      int num_samples = getInputNumItems(0);
      int weight_length = getInputLength(1);
      int item_length = data_length / num_samples;

      // get OpenCL context
      cl_context       context = ocl_env->getContext();
      cl_command_queue command = ocl_env->getCmdQueue();
      cl_program       program = ocl_env->getProgram();

      int err;
      cl_event event;

      // get corresponding cl_kernel
      cl_kernel kernel = clCreateKernel(
          program, "arrayTest", &err);

      if (!kernel || err != CL_SUCCESS) {
        throw std::runtime_error(
            "Failed to create compute kernel arrayTest");
      }

      // get the pointer to input/output data
      cl_mem ocl_data    = *((cl_mem*)getInput(0));
      cl_mem ocl_weights = *((cl_mem*)getInput(1));
      cl_mem ocl_output  = *((cl_mem*)getOutput(
            0, item_length, num_samples,
            sizeof(double)));

      if (!ocl_data || !ocl_weights || !ocl_output) {
        throw std::runtime_error("Buffer are not allocated");
      }

      // Set the arguments to our compute kernel
      err  = clSetKernelArg(kernel, 0, sizeof(int), &weight_length);
      err |= clSetKernelArg(kernel, 1, sizeof(int), &num_samples);
      err |= clSetKernelArg(kernel, 2, sizeof(int), &item_length);
      err |= clSetKernelArg(kernel, 3, sizeof(cl_mem), &ocl_weights);
      err |= clSetKernelArg(kernel, 4, sizeof(cl_mem), &ocl_data);
      err |= clSetKernelArg(kernel, 5, sizeof(cl_mem), &ocl_output);
      if (err != CL_SUCCESS) {
        throw std::runtime_error("Cannot set kernel args\n");
      }

      size_t work_lsize[1] = {128};
      size_t work_gsize[1];
      work_gsize[0] = 64*work_lsize[0];

      err = clEnqueueNDRangeKernel(command, kernel, 1, NULL, work_gsize, work_lsize, 0, NULL, &event);
      clWaitForEvents(1, &event);
    }
    catch (std::runtime_error &e) {
      throw e;
    }
  }
};

extern "C" Task* create() {
  return new ArrayTest();
}

extern "C" void destroy(Task* p) {
  delete p;
}
