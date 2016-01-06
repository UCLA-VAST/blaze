#include <glog/logging.h>

#include "Platform.h"
#include "BlockManager.h"
#include "OpenCLEnv.h"
#include "OpenCLPlatform.h"
#include "OpenCLQueueManager.h"

#define MAX_PLATFORMS 32

namespace blaze {

OpenCLPlatform::OpenCLPlatform() 
  : curr_program(NULL), curr_kernel(NULL)
{
  // start platform setting up
  int err;

  cl_platform_id  platforms[MAX_PLATFORMS];
  cl_device_id    device_id;
  uint32_t        num_platforms = 0;

  // Connect to first platform
  err = clGetPlatformIDs(MAX_PLATFORMS, platforms, &num_platforms);

  if (err != CL_SUCCESS || num_platforms == 0) {
    throw std::runtime_error(
        "No OpenCL platform exists on this host");
  }

  // iterate through all platforms and find NVidia GPU
  int platform_idx = 0;
  for (platform_idx=0; platform_idx<num_platforms; platform_idx++) {
    char cl_platform_name[1001];

    err = clGetPlatformInfo(
        platforms[platform_idx], CL_PLATFORM_NAME, 
        1000, (void *)cl_platform_name, NULL);

    if (err != CL_SUCCESS) {
      LOG(ERROR) << "clGetPlatformInfo(CL_PLATFORM_NAME) "
        << "failed on platform " << platform_idx;;
    }
    if (strstr(cl_platform_name, "Xilinx")!=NULL) {
      // found platform
      break;
    }
  }
  if (platform_idx>=num_platforms) {
    LOG(ERROR) << "No Xilinx platform found, this binary only " <<
      "supports Xilinx FPGAs";
    throw std::runtime_error("No supported platform found");
  }
  DLOG(INFO) << "Found Xilinx OpenCLPlatform at " << platform_idx;

  // Connect to a compute device
  err = clGetDeviceIDs(
      platforms[platform_idx], 
      CL_DEVICE_TYPE_ACCELERATOR, 
      1, 
      &device_id, 
      NULL);

  if (err != CL_SUCCESS) {
    throw std::runtime_error(
        "Failed to create a device group!");
  }

  // Create a compute context 
  cl_context context = clCreateContext(0, 1, 
      &device_id, NULL, NULL, &err);

  if (!context) {
    throw std::runtime_error(
        "Failed to create a compute context!");
  }

  // Create a command commands
  cl_command_queue cmd_queue = clCreateCommandQueue(
      context, device_id, 0, &err);

  if (!cmd_queue) {
    throw std::runtime_error(
        "Failed to create a command queue context!");
  }

  env = new OpenCLEnv(context, cmd_queue, device_id);

  TaskEnv_ptr ep(env);
  env_ptr = ep;

  QueueManager_ptr queue(new OpenCLQueueManager(this)); 
  queue_manager = queue;
}

OpenCLPlatform::~OpenCLPlatform() {

  clReleaseCommandQueue(env->getCmdQueue());
  clReleaseContext(env->getContext());
}

void OpenCLPlatform::createBlockManager(
    size_t cache_limit, 
    size_t scratch_limit) 
{
  BlockManager_ptr bman(new BlockManager(this, 
        cache_limit, scratch_limit));

  block_manager = bman;
}

BlockManager* OpenCLPlatform::getBlockManager() {
  return block_manager.get();
}

void OpenCLPlatform::setupAcc(AccWorker &conf) {

  int err;
  int status = 0;
  size_t n_t = 0;
  unsigned char* kernelbinary;

  // get specific ACC Conf from key-value pair
  std::string program_path;
  std::string kernel_name;

  for (int i=0; i<conf.param_size(); i++) {
    if (conf.param(i).key().compare("program_path")==0) {
      program_path = conf.param(i).value();
    }
    if (conf.param(i).key().compare("kernel_name")==0) {
      kernel_name = conf.param(i).value();
    }
  }
  if (program_path.empty() || kernel_name.empty()) {
    throw std::runtime_error("Invalid configuration");
  }

  DLOG(INFO) << "Load Bitstream from file " << program_path.c_str();

  // Load binary from disk
  int n_i = load_file(
      program_path.c_str(), 
      (char **) &kernelbinary);

  if (n_i < 0) {
    throw std::runtime_error(
        "failed to load kernel from xclbin");
  }
  n_t = n_i;

  // save bitstream
  bitstreams.insert(std::make_pair(
        conf.id(), 
        std::make_pair(n_i, kernelbinary)));

  // save kernel name
  kernel_list.insert(std::make_pair(
        conf.id(), kernel_name));
}

void OpenCLPlatform::changeProgram(std::string acc_id) {

  // NOTE: current version reprograms FPGA everytime a new kernel
  // is required
  // NOTE: there is an assumption that kernel and program are one-to-one mapped
  cl_int err;
  cl_int status;

  uint64_t start_t, elapse_t;

  // check if corresponding kernel is current
  if (curr_acc_id.compare(acc_id) != 0) {

    start_t = getUs();

    // release previous kernel
    if (curr_program && curr_kernel) {
      clReleaseProgram(curr_program);
      clReleaseKernel(curr_kernel);
    }

    elapse_t = getUs() - start_t;
    DLOG(INFO) << "Releasing program and kernel takes " << 
      elapse_t << "us.";

    if (bitstreams.find(acc_id) == bitstreams.end() ||
        kernel_list.find(acc_id) == kernel_list.end()) 
    {
      DLOG(ERROR) << "Bitstream not setup correctly";
      throw std::runtime_error("Cannot find bitstream information");
    }

    // load bitstream from memory
    std::pair<int, unsigned char*> bitstream = bitstreams[acc_id];
    std::string kernel_name = kernel_list[acc_id];

    cl_context context = env->getContext();
    cl_device_id device_id = env->getDeviceId();

    size_t n_t = bitstream.first;
    unsigned char* kernelbinary = bitstream.second;

    // lock OpenCL Context
    boost::lock_guard<OpenCLEnv> guard(*env);

    start_t = getUs();

    // Switch bitstream in FPGA
    cl_program program = clCreateProgramWithBinary(
        context, 1, &device_id, &n_t,
        (const unsigned char **) &kernelbinary, 
        &status, &err);

    if ((!program) || (err!=CL_SUCCESS)) {
      throw std::runtime_error(
          "Failed to create compute program from binary");
    }

    elapse_t = getUs() - start_t;
    DLOG(INFO) << "clCreateProgramWithBinary takes " << 
      elapse_t << "us.";

    start_t = getUs();

    // Create the compute kernel in the program we wish to run
    cl_kernel kernel = clCreateKernel(
        program, kernel_name.c_str(), &err);

    if (!kernel || err != CL_SUCCESS) {
      throw std::runtime_error(
          "Failed to create compute kernel!");
    }

    elapse_t = getUs() - start_t;
    DLOG(INFO) << "clCreateKernel takes " << 
      elapse_t << "us.";

    // setup current accelerator info
    curr_program = program;
    curr_kernel = kernel;
    curr_acc_id = acc_id;

    // switch kernel handler to OpenCLEnv
    env->changeKernel(kernel);

    LOG(INFO) << "Switched to new accelerator: " << acc_id;
  }
}  

cl_kernel& OpenCLPlatform::getKernel() {
  return curr_kernel;
}

TaskEnv_ptr OpenCLPlatform::getEnv(std::string id) {
  return env_ptr; 
}

DataBlock_ptr OpenCLPlatform::createBlock(
    int num_items, 
    int item_length,
    int item_size, 
    int align_width,
    int flag)
{
  DataBlock_ptr block(new OpenCLBlock(env,
        num_items, item_length, item_size, 
        align_width, flag));  

  return block;
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

extern "C" Platform* create() {
  return new OpenCLPlatform();
}

extern "C" void destroy(Platform* p) {
  delete p;
}

} // namespace blaze
