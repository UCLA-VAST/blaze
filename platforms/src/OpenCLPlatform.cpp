#include "OpenCLPlatform.h"

namespace blaze {

#define LOG_HEADER  std::string("OpenCLPlatform::") + \
                    std::string(__func__) +\
                    std::string("(): ")

OpenCLPlatform::OpenCLPlatform() {
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

  env = new OpenCLEnv(context, cmd_queue);
}

void OpenCLPlatform::setupAcc(AccWorker &conf) {

  // NOTE: current version reprograms FPGA everytime a new kernel
  // is required
  // NOTE: there is an assumption that kernel and program are one-to-one mapped
  
  // check if corresponding kernel is current
  if (curr_acc_id == conf.id()) {
    return;
  }
  else if (programs.find(conf.id()) != programs.end()) {

    int err;

    OpenCLEnv* ocl_env = dynamic_cast<OpenCLEnv*>(env);

    // lock OpenCL Context
    boost::lock_guard<OpenCLEnv> guard(*ocl_env);
    
    cl_program program = programs[conf.id()];

    // switch bitstream in FPGA   
    err = clBuildProgram(program, 0, NULL, NULL, NULL, NULL);

    if (err != CL_SUCCESS) {
      throw std::runtime_error(
          "Failed to build program executable!");
    }

    // switch kernel in the TaskEnv
    ocl_env->changeKernel(kernels[conf.id()]);
  }
  else { // required programs have not been loaded yet

    int err;

    // get specific ACC Conf from key-value pair
    std::string program_path;
    std::string kernel_name;
    for (int i=0; i<conf.param_size(); i++) {
      if (conf.param(i).key().compare("ocl_program_path")==0) {
        program_path = conf.param(i).value();
      }
      else if (conf.param(i).key().compare("ocl_kernel_name")==0) {
        kernel_name = conf.param(i).value();
      }
    }

    if (program_path.empty() || kernel_name.empty()) {
      throw std::runtime_error("Invalid configuration");
    }

    OpenCLEnv* ocl_env = dynamic_cast<OpenCLEnv*>(env);

    // lock OpenCL Context
    boost::lock_guard<OpenCLEnv> guard(*ocl_env);

    // Load binary from disk
    unsigned char *kernelbinary;

    int n_i = load_file(
        program_path.c_str(), 
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
        kernel_name.c_str(), 
        &err);

    if (!kernel || err != CL_SUCCESS) {
      throw std::runtime_error(
          "Failed to create compute kernel!");
    }

    // save program handle to the map table
    kernels.insert(std::make_pair(
          conf.id(), 
          kernel));

    // setup kernel in the TaskEnv
    ocl_env->changeKernel(kernel);
  }
}  

int OpenCLPlatform::load_file(
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

} // namespace blaze
