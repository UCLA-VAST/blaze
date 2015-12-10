#include <stdexcept>

#include "Task.h" 
#include "gpu_ocl/OpenCLEnv.h" 

using namespace blaze;

class Logistic : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  Logistic(): Task(2) {;}

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
    int total_length  = getInputLength(0);
    int num_vectors   = getInputNumItems(0);
    int vector_length = total_length / num_vectors;

    // get OpenCL context
    cl_context       context = ocl_env->getContext();
    cl_kernel        kernel  = ocl_env->getKernel();
    cl_command_queue command = ocl_env->getCmdQueue();

    int err;
    cl_event event;

    // get the pointer to input/output data
    cl_mem ocl_a = *((cl_mem*)getInput(0));
    cl_mem ocl_b = *((cl_mem*)getInput(1));
    cl_mem ocl_c = *((cl_mem*)getOutput(
          0, vector_length, num_vectors,
          sizeof(double)));

    if (!ocl_a || !ocl_b || !ocl_c) {
      throw std::runtime_error("Buffer are not allocated");
    }

    // Set the arguments to our compute kernel
    err |= clSetKernelArg(kernel, 0, sizeof(int), &num_vectors);
    err |= clSetKernelArg(kernel, 1, sizeof(int), &vector_length);
    err |= clSetKernelArg(kernel, 2, sizeof(cl_mem), &ocl_a);
    err |= clSetKernelArg(kernel, 3, sizeof(cl_mem), &ocl_b);
    err |= clSetKernelArg(kernel, 4, sizeof(cl_mem), &ocl_c);
    if (err != CL_SUCCESS) {
      throw std::runtime_error("Cannot set kernel args\n");
    }

    size_t work_lsize[1] = {128};
    size_t work_gsize[1];
    work_gsize[0] = 64*work_lsize[0];

    ocl_env->lock();
    err = clEnqueueNDRangeKernel(command, kernel, 1, NULL, work_gsize, work_lsize, 0, NULL, &event);
    ocl_env->unlock();
    clWaitForEvents(1, &event);

    // Execute the kernel over the entire range of our 1d input data set
    // using the maximum number of work group items for this device
    err = clEnqueueTask(command, kernel, 0, NULL, &event);
    if (err) {
      throw std::runtime_error("Failed to execute kernel!");
    }
    clWaitForEvents(1, &event);
  }
};

extern "C" Task* create() {
  return new Logistic();
}

extern "C" void destroy(Task* p) {
  delete p;
}
