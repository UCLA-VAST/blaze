
#include <sys/types.h>
#include <sys/ipc.h>  
#include <sys/shm.h>  
#include <sys/sem.h>  
#include <unistd.h>   
#include <fcntl.h>    
#include <stdio.h>    
#include <stdlib.h>   
#include <string.h>   
#include <math.h>     
#include <unistd.h>   
#include <assert.h>   
#include <stdbool.h>  
#include <sys/types.h>
#include <sys/stat.h> 
#include <CL/opencl.h>

int
load_file_to_memory(const char *filename, char **result)
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

int main(int argc, char *argv[]) {
   
  int err;                            // error code returned from api calls

  cl_platform_id platform_id;         // platform id
  cl_device_id device_id;             // compute device id 
  cl_context context;                 // compute context
  cl_command_queue commands;          // compute command queue
  cl_program program;                 // compute program
  cl_kernel kernel;                   // compute kernel
  cl_event event;										 // event
   
  char cl_platform_vendor[1001];
  char cl_platform_name[1001];

  // Connect to first platform
  //
  err = clGetPlatformIDs(1, &platform_id, NULL);
  if (err != CL_SUCCESS)
  {
    printf("Error: Failed to find an OpenCL platform!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  err = clGetPlatformInfo(platform_id,CL_PLATFORM_VENDOR,
				1000,(void *)cl_platform_vendor,NULL);
  if (err != CL_SUCCESS)
  {
    printf("Error: clGetPlatformInfo(CL_PLATFORM_VENDOR) failed!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  printf("CL_PLATFORM_VENDOR %s\n",cl_platform_vendor);
  err = clGetPlatformInfo(platform_id,CL_PLATFORM_NAME,
				1000,(void *)cl_platform_name,NULL);
  if (err != CL_SUCCESS)
  {
    printf("Error: clGetPlatformInfo(CL_PLATFORM_NAME) failed!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  printf("CL_PLATFORM_NAME %s\n",cl_platform_name);
 
  // Connect to a compute device
  //
  int fpga = 0;
#if defined (FPGA_DEVICE)
  fpga = 1;
#endif
  err = clGetDeviceIDs(platform_id, 
			fpga ? CL_DEVICE_TYPE_ACCELERATOR : CL_DEVICE_TYPE_CPU,
			1, &device_id, NULL);
  if (err != CL_SUCCESS)
  {
    printf("Error: Failed to create a device group!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
	else 
	{
		size_t size;
		err = clGetDeviceInfo(device_id, CL_DEVICE_MAX_WORK_GROUP_SIZE, sizeof(size_t), &size, NULL);
	}
  
  // Create a compute context 
  //
  context = clCreateContext(0, 1, &device_id, NULL, NULL, &err);
  if (!context)
  {
    printf("Error: Failed to create a compute context!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }

  // Create a command commands
  //
  commands = clCreateCommandQueue(context, device_id, 0, &err);
  if (!commands)
  {
    printf("Error: Failed to create a command commands!\n");
    printf("Error: code %i\n",err);
    printf("Test failed\n");
    return EXIT_FAILURE;
  }

  int status;

  // Create Program Objects
  //
  
  // Load binary from disk
  unsigned char *kernelbinary;
  char *xclbin=argv[1];
  printf("loading %s\n", xclbin);
  int n_i = load_file_to_memory(xclbin, (char **) &kernelbinary);
  if (n_i < 0) {
    printf("failed to load kernel from xclbin: %s\n", xclbin);
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  size_t n = n_i;
  // Create the compute program from offline
  program = clCreateProgramWithBinary(context, 1, &device_id, &n,
							(const unsigned char **) &kernelbinary, &status, &err);
  if ((!program) || (err!=CL_SUCCESS)) {
    printf("Error: Failed to create compute program from binary %d!\n", err);
    printf("Test failed\n");
    return EXIT_FAILURE;
  }

  // Build the program executable
  //
  err = clBuildProgram(program, 0, NULL, NULL, NULL, NULL);
  if (err != CL_SUCCESS)
  {
    size_t len;
    char buffer[2048];

    printf("Error: Failed to build program executable!\n");
    clGetProgramBuildInfo(program, device_id, 
				CL_PROGRAM_BUILD_LOG, sizeof(buffer), buffer, &len);
    printf("%s\n", buffer);
    printf("Test failed\n");
    return EXIT_FAILURE;
  }

  // Create the compute kernel in the program we wish to run
  //
  kernel = clCreateKernel(program, "run", &err);
  if (!kernel || err != CL_SUCCESS)
  {
    printf("Error: Failed to create compute kernel!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }

	int dims = 3;
	double data[] = {
		714.215313,312.5831474,226.6679658,
		795.1223308,483.2316382,827.019699,
		287.0280167,589.0996444,629.6339571,
		620.5416752,366.4759483,101.9330746,
		24.44139996,401.1155234,324.7984987,
		369.279928,197.0198983,304.9355644,
		407.5329774,739.4780819,702.857867,
		464.5121837,376.3328839,799.1458264,
		61.9297265,45.22855646,557.3514078,
		235.7981744,633.8715081,842.5673189};
	int data_length = 30;
	int num_data = data_length / dims;

	double centers[] = {
		714.215313,312.5831474,226.6679658,
		464.5121837,376.3328839,799.1458264};
	int centers_length = 6;
	int num_centers = centers_length / dims;

	int output_length = num_data;
	double output[10];

   cl_mem data_buf = clCreateBuffer(context, CL_MEM_READ_WRITE, sizeof(double) * data_length, NULL, &err);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host");
   return EXIT_FAILURE;
   }
   err = clEnqueueWriteBuffer(commands, data_buf, CL_TRUE, 0, sizeof(double) * data_length, data, 0, NULL, NULL);

   err  = clSetKernelArg(kernel, 0, sizeof(cl_mem), &data_buf);
	 err |= clSetKernelArg(kernel, 1, sizeof(int), &num_data);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host");
   return EXIT_FAILURE;
   }

   cl_mem centers_buf = clCreateBuffer(context, CL_MEM_READ_WRITE, sizeof(double) * centers_length, NULL, &err);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host");
   return EXIT_FAILURE;
   }
	 err = clEnqueueWriteBuffer(commands, centers_buf, CL_TRUE, 0, sizeof(double) * centers_length, centers, 0, NULL, NULL);

   err  = clSetKernelArg(kernel, 2, sizeof(cl_mem), &centers_buf);
	 err |= clSetKernelArg(kernel, 3, sizeof(int), &num_centers);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host");
   return EXIT_FAILURE;
   }

	 err = clSetKernelArg(kernel, 4, sizeof(int), &dims);

   cl_mem output_buf = clCreateBuffer(context, CL_MEM_READ_WRITE, sizeof(int) * output_length, NULL, &err);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host");
   return EXIT_FAILURE;
   }

   err  = clSetKernelArg(kernel, 5, sizeof(cl_mem), &output_buf);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host");
   return EXIT_FAILURE;
   }

   cl_event readevent;
	 size_t global = 3;

   err = clEnqueueNDRangeKernel(commands, kernel, 1, NULL,
   		&global, NULL, 0, NULL, &event);
   clWaitForEvents(1, &event);

   err = clEnqueueReadBuffer(commands, output_buf, CL_TRUE, 0, sizeof(int) * output_length, output, 0, NULL, &readevent);
   clWaitForEvents(1, &readevent);
 
   clReleaseProgram(program);
   clReleaseKernel(kernel);
   clReleaseCommandQueue(commands);
   clReleaseContext(context);
 
   return 0;
}
