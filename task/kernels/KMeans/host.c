
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
#include <time.h>

#define acc
#define ITERATION	1

#ifdef acc
#include <CL/opencl.h>

//#include "kernel_cl.h"

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
  if (size != (int) fread(*result, sizeof(char), size, f)) 
  { 
    free(*result);
    return -2; // -2 means file reading fail 
  } 
  fclose(f);
  (*result)[size] = 0;
  return size;
}
#endif

int main(int argc, char *argv[]) {

	int dims = 784;
	double *data;
	int data_length = 60000 * 784;
	int num_data = data_length / dims;

	double *centers;
	int centers_length = 3 * 784;
	int num_centers = centers_length / dims;

	int output_length = num_data;
	int *output;

	data = (double *)malloc(sizeof(double) * data_length);
	centers = (double *)malloc(sizeof(double) * centers_length);
	output = (int *)malloc(sizeof(int) * output_length);

	// Read input data from file
	FILE *infilep = fopen("/curr/diwu/prog/logistic/data/train_data.txt", "r");
	int i, j;
	for (i = 0; i < num_data; ++i) {
		for (j = 0; j < 10; ++j)
			fscanf(infilep, "%lf", &data[i * dims]);
		for (j = 0; j < dims; ++j)
			fscanf(infilep, "%lf", &data[i * dims + j]);
	}
	fclose(infilep);

	srand(99);
	int c = (int) rand() % num_data;
	for (i = 0; i < num_centers; ++i) {
		for (j = 0; j < dims; ++j) {
			centers[i * dims + j] = data[(c + i) * dims + j];
		}
	}

#ifdef acc  
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
/*
	// Create a program with source code (for GPU usage)
  program = clCreateProgramWithSource(context, 1, 
											(const char**)&kernel_cl, NULL, &status);
*/

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
	fprintf(stderr, "kernel ready\n");
	fprintf(stderr, "writing buffers\n");

   cl_mem data_buf = clCreateBuffer(context, CL_MEM_READ_ONLY, sizeof(double) * data_length, NULL, &err);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host 0");
   return EXIT_FAILURE;
   }
   err = clEnqueueWriteBuffer(commands, data_buf, CL_TRUE, 0, sizeof(double) * data_length, data, 0, NULL, NULL);

   err  = clSetKernelArg(kernel, 0, sizeof(cl_mem), &data_buf);
	 err |= clSetKernelArg(kernel, 1, sizeof(int), &num_data);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host 01 %d", err);
   return EXIT_FAILURE;
   }

   cl_mem centers_buf = clCreateBuffer(context, CL_MEM_READ_ONLY, sizeof(double) * centers_length, NULL, &err);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host 2");
   return EXIT_FAILURE;
   }

   err  = clSetKernelArg(kernel, 2, sizeof(cl_mem), &centers_buf);
	 err |= clSetKernelArg(kernel, 3, sizeof(int), &num_centers);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host 23");
   return EXIT_FAILURE;
   }

	 err = clSetKernelArg(kernel, 4, sizeof(int), &dims);

   cl_mem output_buf = clCreateBuffer(context, CL_MEM_WRITE_ONLY, sizeof(int) * output_length, NULL, &err);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host 4");
   return EXIT_FAILURE;
   }

   err  = clSetKernelArg(kernel, 5, sizeof(cl_mem), &output_buf);
   if(err != CL_SUCCESS) {
   printf("Error: OpenCL host 5");
   return EXIT_FAILURE;
   }

	 int iter;
	 for (iter = 0; iter < ITERATION; ++iter) {
		 err = clEnqueueWriteBuffer(commands, centers_buf, CL_TRUE, 0, sizeof(double) * centers_length, centers, 0, NULL, NULL);

	   cl_event readevent;
		 size_t global = 10;

		 fprintf(stderr, "iteration %d\n", iter);

		 clock_t start = clock();
	   err = clEnqueueNDRangeKernel(commands, kernel, 1, NULL,
  	 		(size_t *) &global, NULL, 0, NULL, &event);
	   clWaitForEvents(1, &event);

		 fprintf(stderr, "time: %lf\n", (double) ((clock() - start) / CLOCKS_PER_SEC));

   	 err = clEnqueueReadBuffer(commands, output_buf, CL_TRUE, 0, sizeof(int) * output_length, output, 0, NULL, &readevent);
   	 clWaitForEvents(1, &readevent);

		 for (i = 0; i < centers_length; ++i)
		  centers[i] = 0;

		 int count[3] = {0};
		 for (i = 0; i < output_length; ++i) {
			 int c = output[i];
		 	 count[c]++;
		 	 for (j = 0; j < dims; ++j)
				 centers[c * dims + j] += data[i * dims + j];
		 }
		 for (i = 0; i < num_centers; ++i) {
			 fprintf(stderr, "Center %d (%d) \t", i, count[i]);
			 for (j = 0; j < dims; ++j) {
				 centers[i * dims + j] /= count[i];
//				 fprintf(stderr, "%.3lf\t", centers[i * dims + j]);
		 	 }
		 }
		 fprintf(stderr, "\n");


		 for (i = 0; i < num_centers; ++i) {
			 fprintf(stderr, "Center %d: ", i);
			 for (j = 0; j < 10; ++j) {
				 centers[i * dims + j] = output[i * dims + j];
				 fprintf(stderr, "%.3lf\t", centers[i * dims + j]);
			 }
			 fprintf(stderr, "\n");
		 }
	}
 
	fprintf(stderr, "all done\n");
  clReleaseProgram(program);
  clReleaseKernel(kernel);
  clReleaseCommandQueue(commands);
  clReleaseContext(context);
#else
	int d, iter;

	for (iter = 0; iter < ITERATION; ++iter) {
		fprintf(stderr, "iteration %d\n", iter);
		for (i = 0; i < num_data; ++i) {
			int closest_center = -1;
			double closest_center_dis = 0;
			for (j = 0; j < num_centers; ++j) {
				double dis = 0;

				for (d = 0; d < dims; ++d) {
	 				dis +=  (centers[j * dims + d] - data[i * dims + d]) * 
	         				(centers[j * dims + d] - data[i * dims + d]);  
				}
				if (dis < closest_center_dis || closest_center == -1) { 
	  			closest_center = j;                                   
	  			closest_center_dis = dis;                             
				}                                                       
			}
			output[i] = closest_center;
		}


		for (i = 0; i < centers_length; ++i)
		 centers[i] = 0;

		int count[3] = {0};
		for (i = 0; i < output_length; ++i) {
			int c = output[i];
		 	count[c]++;
		 	for (j = 0; j < dims; ++j)
				centers[c * dims + j] += data[i * dims + j];
		}
		for (i = 0; i < num_centers; ++i) {
			fprintf(stderr, "Center %d (%d) \t", i, count[i]);
			for (j = 0; j < dims; ++j) {
				centers[i * dims + j] /= count[i];
//				fprintf(stderr, "%.3lf\t", centers[i * dims + j]);
		 	}
		}
		fprintf(stderr, "\n");
	}

#endif

   return 0;
}
