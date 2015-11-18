#define LOG_HEADER "OpenCLPlatform"
#include <glog/logging.h>

#include "OpenCLPlatform.h"
#include "OpenCLQueueManager.h"

namespace blaze {

OpenCLPlatform::OpenCLPlatform()
  : prev_program(NULL), prev_kernel(NULL) 
{
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

  // Connect to a compute device
  err = clGetDeviceIDs(
      platform_id, 
      CL_DEVICE_TYPE_GPU, 
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

  env = new OpenCLEnv(context, cmd_queue);
}

QueueManager_ptr OpenCLPlatform::createQueue() {
  QueueManager_ptr queue(new OpenCLQueueManager(this)); 
  return queue;
}

void OpenCLPlatform::setupProgram(std::string acc_id) {

  // NOTE: current version reprograms FPGA everytime a new kernel
  // is required
  // NOTE: there is an assumption that kernel and program are one-to-one mapped
  
  // check if corresponding kernel is current
  if (curr_acc_id != acc_id) {

    OpenCLEnv* ocl_env = (OpenCLEnv*)env;
    AccWorker conf = acc_table[acc_id];

    int err;
    int status = 0;
    size_t n_t = 0;
    char* kernelSource;

    // release previous kernel
    if (prev_program && prev_kernel) {
      clReleaseProgram(prev_program);
      clReleaseKernel(prev_kernel);
    }

    // get opencl kernel name and program path
    if (!conf.has_kernel_path() || 
        !conf.has_kernel_name()) 
    {
      throw std::runtime_error("Invalid configuration");
    }
    std::string program_path = conf.kernel_path();
    std::string kernel_name  = conf.kernel_name();

    if (bitstreams.find(acc_id) != bitstreams.end()) {

      DLOG(INFO) << "Binary already stored in memory";

      // Load bitstream from memory
      std::pair<int, char*> bitstream = bitstreams[acc_id];

      n_t = bitstream.first;
      kernelSource = bitstream.second;
    }
    else { // required programs have not been loaded yet

      DLOG(INFO) << "Load program from file " << program_path.c_str();

      // Load binary from disk
      int n_i = load_file(
          program_path.c_str(), 
          &kernelSource);

      if (n_i < 0) {
        throw std::runtime_error(
            "failed to load kernel from xclbin");
      }
      n_t = n_i;

      // save bitstream
      bitstreams.insert(std::make_pair(
            conf.id(), 
            std::make_pair(n_i, kernelSource)));
    }

    // lock OpenCL Context
    boost::lock_guard<OpenCLEnv> guard(*ocl_env);

    cl_program program = clCreateProgramWithSource(context, 1,
            (const char **) &kernelSource, &n_t, &err);

    if ((!program) || (err!=CL_SUCCESS)) {
      throw std::runtime_error(
          "Failed to create compute program from source");
    }

    // Build the program executable
    err = clBuildProgram(program, 0, NULL, NULL, NULL, NULL);

    if (err != CL_SUCCESS) {
      // Determine the size of the log
      size_t log_size;
      clGetProgramBuildInfo(program, device_id, CL_PROGRAM_BUILD_LOG, 0, NULL, &log_size);

      // Allocate memory for the log
      char *log = (char *) malloc(log_size);

      // Get the log
      clGetProgramBuildInfo(program, device_id, CL_PROGRAM_BUILD_LOG, log_size, log, NULL);

      // Print the log
      LOG(ERROR) << log;
      throw std::runtime_error("Failed to build program executable!");
    }

    // Create the compute kernel in the program we wish to run
    cl_kernel kernel = clCreateKernel(
        program, 
        kernel_name.c_str(), 
        &err);

    if (!kernel || err != CL_SUCCESS) {
      throw std::runtime_error(
          "Failed to create compute kernel!");
    }

    prev_program = program;
    prev_kernel = kernel;

    // setup kernel in the TaskEnv
    ocl_env->changeKernel(kernel);

    LOG(INFO) << "Switched to new accelerator: " << acc_id;

    // set current acc_id
    curr_acc_id = acc_id;
  }
}  

int OpenCLPlatform::load_file(
    const char *filename, 
    char **result)
{ 
  int size = 0;
  FILE *f = fopen(filename, "r");
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

extern "C" Platform* create() {
  return new OpenCLPlatform();
}

extern "C" void destroy(Platform* p) {
  delete p;
}

} // namespace blaze
