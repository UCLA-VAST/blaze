#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>

#include "blaze/Task.h" 
#include "OpenCLEnv.h" 

using namespace blaze;

class Memcpy : public Task {
public:

  // extends the base class constructor to indicate 
  // that there is one input block for this task
  Memcpy(): Task(1) {;}

  // overwrites the compute function
  virtual void compute() {

    // get OpenCL environment from runtime
    OpenCLEnv* ocl_env = (OpenCLEnv*)getEnv();

    // get OpenCL context
    cl_context       context = ocl_env->getContext();
    cl_kernel        kernel  = ocl_env->getKernel();
    cl_command_queue command = ocl_env->getCmdQueue();

    // get input data length
    int data_length = getInputLength(0);

    // get number of input items
    int num_items = getInputNumItems(0);

    // get the pointer to input/output data
    cl_mem src_data = *(cl_mem*)getInput(0);
    cl_mem dst_data = *(cl_mem*)getOutput(
        0, data_length/num_items, num_items, sizeof(double));

    if (!src_data || !dst_data) {
      throw std::runtime_error("Cannot get data pointers");
    }
    
    int err;
    cl_event event;

    // execute OpenCL kernel, with the equivalent function of: 
    // memcpy(dst_data, src_data, data_length*sizeof(double))
    err  = clSetKernelArg(kernel, 0, sizeof(cl_mem), &dst_data);
    err |= clSetKernelArg(kernel, 1, sizeof(cl_mem), &src_data);
    err |= clSetKernelArg(kernel, 2, sizeof(int), &data_length);

    // current Xilinx OpenCL is not thread-safe, so mutex on 
    // command queue is required
    ocl_env->lock();

    // start kernel
    err = clEnqueueTask(command, kernel, 0, NULL, &event);

    // unlock mutex
    ocl_env->unlock();

    if (err) {
      throw("Failed to execute kernel!");
    }

    // wait for kernel to finish
    clWaitForEvents(1, &event);
  }
};

extern "C" Task* create() {
  return new Memcpy();
}

extern "C" void destroy(Task* p) {
  delete p;
}
