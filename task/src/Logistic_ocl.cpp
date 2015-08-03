#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>

//#define USEMKL

#ifdef USEMKL
#include <mkl.h>
#endif

#include "acc_runtime.h" 

using namespace acc_runtime;

#define LABEL_SIZE		10
#define FEATURE_SIZE	784

class Logistic : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  Logistic(TaskEnv* env): Task(env, 2) {;}

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

    // dynamically cast the TaskEnv to OpenCLTen
    OpenCLEnv* ocl_env = dynamic_cast<OpenCLEnv*>(env);

    // get input data length
    //int nsample = getInputNumItems(0);
    int data_length = getInputLength(0);
    int weight_length = getInputLength(1);

    //printf("data length = %d\n", data_length);
    //printf("weight length = %d\n", weight_length);

    // check input size
    if (data_length % (LABEL_SIZE+FEATURE_SIZE) != 0 || 
        data_length / (LABEL_SIZE+FEATURE_SIZE) == 0 ||
        weight_length != (LABEL_SIZE*FEATURE_SIZE))
    {
      fprintf(stderr, "Invalid input data dimensions\n");
      throw std::runtime_error("Invalid input data dimensions");
    }

    // get the pointer to input/output data
    float* data      = (float*)getInput(0);
    float* weights   = (float*)getInput(1);
    float* gradients = (float*)getOutput(
        0, 
        weight_length, 
        sizeof(float));

    if (!data || !weights || !gradients) {
      fprintf(stderr, "Cannot get data pointers\n");
      throw std::runtime_error("Cannot get data pointers");
    }

    float label[LABEL_SIZE];

    // perform computation
    int nsample = data_length / 
      (LABEL_SIZE+FEATURE_SIZE);

    //printf("processing %d data points\n", nsample);

    int L = LABEL_SIZE;
    int D = FEATURE_SIZE;

    int m = LABEL_SIZE;
    int n = FEATURE_SIZE;
    int inc = 1;
    float alpha = 1.0f;
    float beta = .0f;

    memset(gradients, 0, sizeof(float)*LABEL_SIZE*FEATURE_SIZE);

    cl_context       context = ocl_env->getContext();
    cl_kernel        kernel  = ocl_env->getKernel("Logistic");
    cl_command_queue command = ocl_env->getCmdQueue();

    cl_event event;

    gettimeofday(&t1, NULL);
    // Create the input and output arrays in device memory for our calculation
    cl_mem input_weights = clCreateBuffer(
        context, CL_MEM_READ_ONLY,  
        sizeof(float)*L*(D+1), NULL, NULL);

    cl_mem input_data = clCreateBuffer(
        context, CL_MEM_READ_ONLY,  
        sizeof(float)*nsample*(D+L), NULL, NULL);

    cl_mem output_gradient = clCreateBuffer(
        context, CL_MEM_WRITE_ONLY, 
        sizeof(float)*L*(D+1), NULL, NULL);

    int err = 0;
    err |= clEnqueueWriteBuffer(
        command, input_weights, CL_TRUE, 0, 
        sizeof(float)*L*D, weights, 0, NULL, NULL);

    err |= clEnqueueWriteBuffer(
        command, input_data, CL_TRUE, 0, 
        sizeof(float) * nsample*(D+L), data, 0, NULL, &event);

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to write to source array");
    }
    gettimeofday(&t2, NULL);
    timersub(&t1, &t2, &tr);
    clWaitForEvents(1, &event);
    fprintf(stdout, "Data input takes %.4f ms\n", 
        fabs(tr.tv_sec*1000.0+(double)tr.tv_usec/1000.0));

    // Set the arguments to our compute kernel
    err  = clSetKernelArg(kernel, 0, sizeof(int), &nsample);
    err |= clSetKernelArg(kernel, 1, sizeof(cl_mem), &input_weights);
    err |= clSetKernelArg(kernel, 2, sizeof(cl_mem), &input_data);
    err |= clSetKernelArg(kernel, 3, sizeof(cl_mem), &output_gradient);

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to set kernel arguments!");
    }

    gettimeofday(&t1, NULL);
    // Execute the kernel over the entire range of our 1d input data set
    // using the maximum number of work group items for this device

    err = clEnqueueTask(command, kernel, 0, NULL, &event);
    clWaitForEvents(1, &event);

    if (err) {
      throw("Failed to execute kernel!");
    }

    gettimeofday(&t2, NULL);
    timersub(&t1, &t2, &tr);
    fprintf(stdout, "FPGA execution takes %.4f ms\n", 
        fabs(tr.tv_sec*1000.0+(double)tr.tv_usec/1000.0));

    gettimeofday(&t1, NULL);
    // Read back the results from the device to verify the output
    err = clEnqueueReadBuffer(
        command, 
        output_gradient, 
        CL_TRUE, 0, sizeof(float)*L*D, 
        gradients, 0, NULL, &event);  

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to read output array");
    }
    clWaitForEvents(1, &event);

    gettimeofday(&t2, NULL);
    timersub(&t1, &t2, &tr);
    fprintf(stdout, "Data output takes %.4f ms\n", 
        fabs(tr.tv_sec*1000.0+(double)tr.tv_usec/1000.0));

    clReleaseMemObject(input_weights);
    clReleaseMemObject(input_data);
    clReleaseMemObject(output_gradient);
  }
};

extern "C" Task* create(TaskEnv* env) {
  return new Logistic(env);
}

extern "C" void destroy(Task* p) {
  delete p;
}
