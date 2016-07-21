#define LOG_HEADER "OpenCLPlatform"
#include <glog/logging.h>

#include "blaze/BlockManager.h"
#include "blaze/nv_opencl/OpenCLBlock.h"
#include "blaze/nv_opencl/OpenCLEnv.h"
#include "blaze/nv_opencl/OpenCLPlatform.h"
#include "blaze/nv_opencl/OpenCLQueueManager.h"

#define MAX_PLATFORMS 32

namespace blaze {

OpenCLPlatform::OpenCLPlatform(
    std::map<std::string, std::string> &conf_table
    ): Platform(conf_table)
{
  int err;

  // start platform setting up
  cl_platform_id  platforms[MAX_PLATFORMS];
  cl_device_id    devices[MAX_PLATFORMS];
  uint32_t        num_platforms = 0;

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
    if (strstr(cl_platform_name, "NVIDIA")!=NULL) {
      // found platform
      break;
    }
  }
  if (platform_idx>=num_platforms) {
    LOG(ERROR) << "No NVidia platform found, this binary only " <<
      "supports NVidia GPUs";
    throw std::runtime_error("No supported platform found");
  }

  // Connect to a compute device
  err = clGetDeviceIDs(
      platforms[platform_idx], CL_DEVICE_TYPE_GPU, 
      MAX_PLATFORMS, devices, &num_devices);

  if (err != CL_SUCCESS) {
    throw std::runtime_error(
        "Failed to create a device group!");
  }

  DLOG(INFO) << "Found " << num_devices << " GPUs";

  for (int d=0; d<num_devices; d++) {

    // Create a compute context for each device
    cl_context context = clCreateContext(0, 1, 
        &devices[d], NULL, NULL, &err);

    if (!context) {
      throw std::runtime_error(
          "Failed to create a compute context");
    }

    // Create command queues
    cl_command_queue cmd_queue = clCreateCommandQueue(
        context, devices[d], 0, &err);

    if (!cmd_queue) {
      throw std::runtime_error(
          "Failed to create a command queue");
    }

    env_list.push_back(new OpenCLEnv(d, context, cmd_queue, devices[d]));
  }

  // create QueueManager
  QueueManager_ptr queue(new OpenCLQueueManager(this)); 
  queue_manager = queue;
}

OpenCLPlatform::~OpenCLPlatform() {

  for (std::map<std::string, std::vector<cl_program> >::iterator 
      iter = program_list.begin(); 
      iter != program_list.end(); 
      iter ++) 
  {
    std::vector<cl_program> programs = iter->second;
    for (int d=0; d<num_devices; d++) {
      clReleaseProgram(programs[d]);
    }
  }

  for (std::vector<OpenCLEnv*>::iterator iter = env_list.begin(); 
      iter != env_list.end(); iter ++) 
  {
    clReleaseCommandQueue((*iter)->getCmdQueue());
    clReleaseContext((*iter)->getContext());
  }
}

int OpenCLPlatform::getNumDevices() {
  return num_devices;
}

TaskEnv_ptr OpenCLPlatform::getEnv(std::string id) {

  TaskEnv_ptr taskEnv(new OpenCLTaskEnv(
        env_list, program_list[id]));
  
  return taskEnv;
}

OpenCLEnv* OpenCLPlatform::getEnv(int id) {
  if (id<0 || id>=num_devices) {
    return NULL; 
  }
  else {
    return env_list[id];
  }
}

DataBlock_ptr OpenCLPlatform::createBlock(
    int num_items, 
    int item_length,
    int item_size, 
    int align_width,
    int flag)
{
  DataBlock_ptr block(
      new OpenCLBlock(env_list,
        num_items, item_length, item_size, align_width, flag)
      );  
  return block;
}

DataBlock_ptr OpenCLPlatform::createBlock(const OpenCLBlock& block)
{
  DataBlock_ptr bp(
      new OpenCLBlock(block));
  return bp;
}

void OpenCLPlatform::addQueue(AccWorker &conf) {

  // add a TaskManager, and the scheduler should be started
  queue_manager->add(conf.id(), conf.path());

  int err;
  int status = 0;
  size_t n_t = 0;
  char* kernelSource;

  std::string acc_id = conf.id();
  std::string program_path;

  // get specific ACC Conf from key-value pair
  for (int i=0; i<conf.param_size(); i++) {
    if (conf.param(i).key().compare("program_path")==0) {
      program_path = conf.param(i).value();
    }
  }
  if (program_path.empty()) {
    throw std::runtime_error("Invalid configuration");
  }

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

  std::vector<cl_program> programs;

  // build program for each device context
  for (int d=0; d<num_devices; d++) {

    // get context and device_id from the first env
    cl_context   context   = env_list[d]->getContext();
    cl_device_id device_id = env_list[d]->getDeviceId();

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
      clGetProgramBuildInfo(program, device_id, CL_PROGRAM_BUILD_LOG, 
          0, NULL, &log_size);

      // Allocate memory for the log
      char *log = (char *) malloc(log_size);

      // Get the log
      clGetProgramBuildInfo(program, device_id, CL_PROGRAM_BUILD_LOG, 
          log_size, log, NULL);

      // Print the log
      LOG(ERROR) << log;

      throw std::runtime_error("Failed to build program executable!");
    }
    programs.push_back(program);
  }
  program_list.insert(std::make_pair(acc_id, programs));
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

extern "C" Platform* create(
    std::map<std::string, std::string> &conf_table) 
{
  return new OpenCLPlatform(conf_table);
}

extern "C" void destroy(Platform* p) {
  delete p;
}

} // namespace blaze
