#ifndef OPENCLENV_H
#define OPENCLENV_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>

#include <CL/opencl.h>

#include "proto/acc_conf.pb.h"
#include "TaskEnv.h"

namespace blaze {

//class OpenCLBlock;

class OpenCLEnv : public TaskEnv 
{
public:
  
  // TODO: need to differentiate GPU and FPGA OpenCL
  OpenCLEnv(AccType type): TaskEnv(type) {

    // start platform setting up
    int err;

    cl_platform_id platform_id;

    char cl_platform_vendor[1001];
    char cl_platform_name[1001];

    // Connect to first platform
    err = clGetPlatformIDs(1, &platform_id, NULL);

    if (err != CL_SUCCESS) {
      throw std::runtime_error(
          "Failed to find an OpenCL platform!");
    }

    err = clGetPlatformInfo(
        platform_id, 
        CL_PLATFORM_VENDOR, 
        1000, 
        (void *)cl_platform_vendor,NULL);

    if (err != CL_SUCCESS) {
      throw std::runtime_error(
          "clGetPlatformInfo(CL_PLATFORM_VENDOR) failed!");
    }

    err = clGetPlatformInfo(
        platform_id,
        CL_PLATFORM_NAME,
        1000,
        (void *)cl_platform_name,
        NULL);

    if (err != CL_SUCCESS) {
      throw std::runtime_error(
          "clGetPlatformInfo(CL_PLATFORM_NAME) failed!");
    }

    // Connect to a compute device
    err = clGetDeviceIDs(
        platform_id, 
        CL_DEVICE_TYPE_ACCELERATOR, 
        1, 
        &device_id, 
        NULL);

    if (err != CL_SUCCESS) {
      throw std::runtime_error(
          "Failed to create a device group!");
    }

    // Create a compute context 
    context = clCreateContext(0, 1, &device_id, NULL, NULL, &err);

    if (!context) {
      throw std::runtime_error(
          "Failed to create a compute context!");
    }

    // Create a command commands
    cmd_queue = clCreateCommandQueue(context, device_id, 0, &err);

    if (!cmd_queue) {
      throw std::runtime_error(
          "Failed to create a command queue context!");
    }
  }

  // TODO: the current version will reprogram FPGA everytime
  // setup is called
  virtual void setup(AccWorker &conf)
  {
    int err;

    // check if corresponding kernel has already been setup
    if (kernels.find(conf.id()) != kernels.end()) {
      return; 
    }
    // Create Program Objects

    // Load binary from disk
    unsigned char *kernelbinary;
    
    int n_i = load_file(
        conf.impl_path().c_str(), 
        (char **) &kernelbinary);

    if (n_i < 0) {
        throw std::runtime_error(
            "failed to load kernel from xclbin");
    }
    size_t n_t = n_i;

    int status = 0;

    // Create the compute program from offline
    cl_program program = clCreateProgramWithBinary(
        context, 1, &device_id, &n_t,
        (const unsigned char **) &kernelbinary, 
        &status, &err);

    if ((!program) || (err!=CL_SUCCESS)) {
        throw std::runtime_error(
            "Failed to create compute program from binary");
    }

    // Build the program executable
    err = clBuildProgram(program, 0, NULL, NULL, NULL, NULL);

    if (err != CL_SUCCESS) {
        throw std::runtime_error(
            "Failed to build program executable!");
    }

    // save program handle to the map table
    programs.insert(std::make_pair(
          conf.id(), 
          program));

    // Create the compute kernel in the program we wish to run
    cl_kernel kernel = clCreateKernel(
        program, 
        conf.kernel_name().c_str(), 
        &err);

    if (!kernel || err != CL_SUCCESS) {
        throw std::runtime_error(
            "Failed to create compute kernel!");
    }

    // save program handle to the map table
    kernels.insert(std::make_pair(
          conf.id(), 
          kernel));

  }

  ~OpenCLEnv() {

      for (std::map<std::string, cl_program>::iterator 
             iter = programs.begin(); 
           iter != programs.end(); 
           iter ++) 
      {
        clReleaseProgram(iter->second);
      }

      for (std::map<std::string, cl_kernel>::iterator 
             iter = kernels.begin(); 
           iter != kernels.end(); 
           iter ++) 
      {
        clReleaseKernel(iter->second);
      }
           
      clReleaseCommandQueue(cmd_queue);
      clReleaseContext(context);
  }

  cl_context& getContext() {
    return context;
  }

  cl_command_queue& getCmdQueue() {
    return cmd_queue;
  }

  cl_kernel& getKernel(std::string acc_id) {
    if (kernels.find(acc_id) == kernels.end()) 
    {
      throw std::runtime_error(acc_id + " not initialized");
    }
    else {
      return kernels[acc_id];
    }
  }

private:

  int load_file(
      const char *filename, 
      char **result)
  { 
    int size = 0;
    FILE *f = fopen(filename, "rb");
    if (f == NULL) 
    { 
      *result = NULL;
      return -1; // -1 means file opening fail 
    } 
    fseek(f, 0, SEEK_END);
    size = ftell(f);
    fseek(f, 0, SEEK_SET);
    *result = (char *)malloc(size+1);
    if (size != fread(*result, sizeof(char), size, f)) 
    { 
      free(*result);
      return -2; // -2 means file reading fail 
    } 
    fclose(f);
    (*result)[size] = 0;
    return size;
  }

  cl_device_id     device_id;
  cl_context       context;
  cl_command_queue cmd_queue;
  //cl_program program;
  //cl_kernel kernel;

  std::map<std::string, cl_program>       programs;
  std::map<std::string, cl_kernel>        kernels;
};
}

#endif 
