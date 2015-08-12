CC=gcc

OPENCL_ROOT = /usr/local/cuda
#OPENCL_ROOT = /opt/AMDAPP/
EXEDIR	= .
EXEC = exe
LINK = -O3 -lm -I$(OPENCL_ROOT)/include -L$(OPENCL_ROOT)/lib64 -lOpenCL
#LINK = -O3 -lm -I$(OPENCL_ROOT)/include -L$(OPENCL_ROOT)/lib/x86_64 -lOpenCL

all: $(EXEDIR)/$(EXEC) 

$(EXEDIR)/$(EXEC): host_gpu.c kernel_gpu_cl.h
	$(CC) -o $(EXEDIR)/$(EXEC) host_gpu.c $(LINK)

kernel_gpu_cl.h: kernel_gpu.cl
	./cl_trans.py kernel_gpu.cl

run:
	./$(EXEC)

clean:
	rm -rf $(EXEDIR)/$(EXEC)

