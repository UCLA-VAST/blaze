#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>
#include <CL/opencl.h>

#include "acc_runtime.h"
#include "../kernels/KMeans_GPU/kernel_gpu_cl.h"

using namespace acc_runtime;

class KMeans : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  KMeans(TaskEnv* env): Task(env, 3) {;}

  // overwrites the readLine runction
  virtual char* readLine(
      std::string line, 
      size_t &num_elements, 
      size_t &num_bytes) 
  {
    std::vector<double>* v = new std::vector<double>;

    std::istringstream iss(line);

    std::copy(std::istream_iterator<double>(iss),
        std::istream_iterator<double>(),
        std::back_inserter(*v));

		size_t dims = v->size();
	  num_bytes = dims * sizeof(double);
    num_elements = dims;

    return (char*) v;
  }

  // overwrites the compute function
  virtual void compute() {

    struct	timeval t1, t2, tr;

    // dynamically cast the TaskEnv to OpenCLTen
    OpenCLEnv* ocl_env = dynamic_cast<OpenCLEnv*>(env);

    // OpenCL setup
    cl_context context = ocl_env->getContext();
    cl_kernel  kernel  = ocl_env->getKernel("KMeans");
    cl_command_queue command = ocl_env->getCmdQueue();

    int err;
    cl_event event;

    // get input data length
    int data_length = getInputLength(0);
    int	center_length = getInputLength(1);

    // get the pointer to input/output data
		cl_mem data     = *((cl_mem*) getInput(0));
    cl_mem centers  = *((cl_mem*) getInput(1));
		int dims = *((int*) getInput(2));

		// calculate # of data
		int num_data = data_length / dims;
		int num_centers = center_length / dims;

    cl_mem closest_centers = *((cl_mem*) 
			getOutput(0, 1, num_data, sizeof(int)));

		// TODO: Check available

    gettimeofday(&t1, NULL);

    // Set input data and length arguments
    err  = clSetKernelArg(kernel, 0, sizeof(cl_mem), &data);
		err |= clSetKernelArg(kernel, 1, sizeof(int), &num_data);
    err |= clSetKernelArg(kernel, 2, sizeof(cl_mem), &centers);
		err |= clSetKernelArg(kernel, 3, sizeof(int), &num_centers);
    err |= clSetKernelArg(kernel, 4, sizeof(int), &dims);

		// Set output data and length arguments
    err |= clSetKernelArg(kernel, 5, sizeof(cl_mem), &closest_centers);

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to set kernel arguments!");
    }

    gettimeofday(&t1, NULL);
		size_t global = num_data * 1024;
		size_t local = 1024;

    // Execute the kernel over the entire range of our 1d input data set
    // using the maximum number of work group items for this device
		err = clEnqueueNDRangeKernel(command, kernel, 1, NULL,
			   (size_t *) &global, (size_t *) &local, 0, NULL, &event);

    if (err) {
      throw("Failed to execute kernel!");
    }

    gettimeofday(&t2, NULL);
    timersub(&t1, &t2, &tr);
    fprintf(stdout, "FPGA execution takes %.4f ms\n", 
        fabs(tr.tv_sec*1000.0+(double)tr.tv_usec/1000.0));

    gettimeofday(&t1, NULL);
  }
};

extern "C" Task* create(TaskEnv* env) {
  return new KMeans(env);
}

extern "C" void destroy(Task* p) {
  delete p;
}
