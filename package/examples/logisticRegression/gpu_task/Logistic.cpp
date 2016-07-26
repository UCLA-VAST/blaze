#include <stdexcept>
#include <string>

#include "blaze/Task.h" 
#include "blaze/nv_opencl/OpenCLEnv.h" 

#define GROUP_SIZE 64
#define GROUP_NUM  256

using namespace blaze;

class Logistic_GPU : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  Logistic_GPU(): Task(2) {;}

  // overwrites the compute function
  // Input data:
  // - data: layout as num_samples x [double label, double[] feature]
  // - weight: (num_labels-1) x feature_length
  // Output data:
  // - gradient plus loss: [double[] gradient, double loss]
  virtual void compute() {

    // dynamically cast the TaskEnv to OpenCLTaskEnv
    OpenCLTaskEnv* ocl_env = (OpenCLTaskEnv*)getEnv();

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
    cl_command_queue command = ocl_env->getCmdQueue();
    cl_program       program = ocl_env->getProgram();

    int err;
    cl_event event;

    // get corresponding cl_kernel
    cl_kernel kernel_compute = clCreateKernel(program, "gradient", &err);
    if (!kernel_compute || err != CL_SUCCESS) {
      throw std::runtime_error(
          "Failed to create compute kernel gradient");
    }

    cl_kernel kernel_sum = clCreateKernel(program, "vector_sum", &err);
    if (!kernel_sum || err != CL_SUCCESS) {
      throw std::runtime_error(
          "Failed to create compute kernel vector_sum");
    }

    // get the pointer to input/output data
    cl_mem ocl_data    = *((cl_mem*)getInput(0));
    cl_mem ocl_weights = *((cl_mem*)getInput(1));
    cl_mem ocl_output  = *((cl_mem*)getOutput(
          0, weight_length+1, 1,
          sizeof(double)));

    if (!ocl_data || !ocl_weights || !ocl_output) {
      throw std::runtime_error("Buffer are not allocated");
    }

    cl_mem ocl_temp = clCreateBuffer(context, CL_MEM_READ_WRITE, 
        GROUP_NUM*(weight_length+1)*sizeof(double), NULL, &err);

    if (!ocl_temp || err != CL_SUCCESS) {
      throw std::runtime_error("Cannot create ocl_temp buffer");
    }

    // Set the arguments of gradient kernel
    err  = clSetKernelArg(kernel_compute, 0, sizeof(int), &num_samples);
    err |= clSetKernelArg(kernel_compute, 1, sizeof(int), &num_labels);
    err |= clSetKernelArg(kernel_compute, 2, sizeof(int), &feature_length);
    err |= clSetKernelArg(kernel_compute, 3, sizeof(cl_mem), &ocl_weights);
    err |= clSetKernelArg(kernel_compute, 4, sizeof(cl_mem), &ocl_data);
    err |= clSetKernelArg(kernel_compute, 5, sizeof(cl_mem), &ocl_temp);
    if (err != CL_SUCCESS) {
      throw std::runtime_error("Cannot set gradient args");
    }

    // Set the arguments of vector_sum kernel
    int vector_size = weight_length+1;
    int num_vectors = GROUP_NUM;

    err  = clSetKernelArg(kernel_sum, 0, sizeof(int), &vector_size);
    err |= clSetKernelArg(kernel_sum, 1, sizeof(int), &num_vectors);
    err |= clSetKernelArg(kernel_sum, 2, sizeof(cl_mem), &ocl_temp);
    err |= clSetKernelArg(kernel_sum, 3, sizeof(cl_mem), &ocl_output);
    if (err != CL_SUCCESS) {
      throw std::runtime_error("Cannot set vector_sum args");
    }

    // setup workgroup dimensions
    size_t work_lsize[1];
    size_t work_gsize[1];

    work_lsize[0] = GROUP_SIZE;
    work_gsize[0] = GROUP_NUM*work_lsize[0];

    err = clEnqueueNDRangeKernel(command, kernel_compute, 1, NULL, 
        work_gsize, work_lsize, 0, NULL, &event);

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to enqueue gradient kernel");
    }

    work_lsize[0] = 1024;
    work_gsize[0] = work_lsize[0];

    err = clEnqueueNDRangeKernel(command, kernel_sum, 1, NULL, 
        work_gsize, work_lsize, 0, NULL, &event);

    if (err) {
      throw std::runtime_error("Failed to enqueue vector_sum kernel");
    }
    clWaitForEvents(1, &event);

    // clean up
    clReleaseMemObject(ocl_temp);
    clReleaseKernel(kernel_compute);
    clReleaseKernel(kernel_sum);
  }
};

extern "C" Task* create() {
  return new Logistic_GPU();
}

extern "C" void destroy(Task* p) {
  delete p;
}
