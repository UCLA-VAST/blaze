include ../../Makefile.config

# check all variables
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif

CUDA_DIR=/usr/local/cuda

PP=g++
CC=gcc

MANAGER_DIR=../../manager

CFLAGS= -c -fPIC -std=c++0x
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/src \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(GLOG_DIR)/include \
				 -I$(XILINX_OPENCL_DIR)/runtime/include/1_2
				 
LINK= -L$(MANAGER_DIR)/lib -lblaze \
			-L$(XILINX_OPENCL_DIR)/runtime/lib/x86_64 -lOpenCL \
			-lpthread -lm -ldl
#-L$(CUDA_DIR)/lib64 -lOpenCL \

DEPS= OpenCLPlatform.o \
			OpenCLTaskEnv.o \
			OpenCLBlock.o \
			OpenCLQueueManager.o

all: gpu-ocl.so

gpu-ocl.so: $(DEPS)
	$(PP) -shared -o $@ $(DEPS) $(LINK) 

%.o: %.cpp *.h Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf ../lib/gpu-ocl.so
