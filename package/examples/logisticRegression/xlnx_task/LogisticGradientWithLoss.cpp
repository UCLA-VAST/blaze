#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <sstream>

#include "blaze/Task.h" 
#include "blaze/xlnx_opencl/OpenCLEnv.h" 

using namespace blaze;

class Logistic_FPGA : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  Logistic_FPGA(): Task(2) {
    addConfig(0, "align_width", "64");
  }

  // overwrites the compute function
  // Input data:
  // - data: layout as num_samples x [double label, double[] feature]
  // - weight: (num_labels-1) x feature_length
  // Output data:
  // - gradient plus loss: [double[] gradient, double loss]
  virtual void compute() {

    struct	timeval t1, t2, tr;

    // dynamically cast the TaskEnv to OpenCLEnv
    OpenCLEnv* ocl_env = (OpenCLEnv*)getEnv();

    // get input data length
    int data_length = getInputLength(0);
    int num_samples = getInputNumItems(0);
    int weight_length = getInputLength(1);
    int feature_length = data_length / num_samples - 1;
    int num_labels = weight_length / feature_length + 1;

    // check input size
    if (weight_length % feature_length != 0 || 
        num_labels < 2 ) 
    {
      fprintf(stderr, "Invalide dimensions\n");
      fprintf(stderr, "num_samples=%d, feature_length=%d, weight_length=%d\n", 
          num_samples, feature_length, weight_length);
      throw std::runtime_error("Invalid input data dimensions");
    }

    // get OpenCL context
    cl_context       context = ocl_env->getContext();
    cl_kernel        kernel  = ocl_env->getKernel();
    cl_command_queue command = ocl_env->getCmdQueue();

    int err;
    cl_event event;

    // get the pointer to input/output data
    cl_mem ocl_data    = *((cl_mem*)getInput(0));
    cl_mem ocl_weights = *((cl_mem*)getInput(1));
    cl_mem ocl_output  = *((cl_mem*)getOutput(
          0, weight_length+1, 1,
          sizeof(double)));

    if (!ocl_data || !ocl_weights || !ocl_output) {
      throw std::runtime_error("Buffer are not allocated");
    }

    // Set the arguments to our compute kernel
    err  = clSetKernelArg(kernel, 0, sizeof(int), &num_samples);
    err |= clSetKernelArg(kernel, 1, sizeof(int), &num_labels);
    err |= clSetKernelArg(kernel, 2, sizeof(int), &feature_length);
    err |= clSetKernelArg(kernel, 3, sizeof(cl_mem), &ocl_weights);
    err |= clSetKernelArg(kernel, 4, sizeof(cl_mem), &ocl_data);
    err |= clSetKernelArg(kernel, 5, sizeof(cl_mem), &ocl_output);
    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to set gradients!");
    }

    // Lock cl_command_queue to ensure thread-safety (Xililnx platform only)
    ocl_env->lock();
    err = clEnqueueTask(command, kernel, 0, NULL, &event);
    ocl_env->unlock();
    clWaitForEvents(1, &event);

    if (err) {
      throw("Failed to execute kernel!");
    }
  }
};

extern "C" Task* create() {
  return new Logistic_FPGA();
}

extern "C" void destroy(Task* p) {
  delete p;
}
