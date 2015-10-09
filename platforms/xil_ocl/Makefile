include ../../Makefile.config

# check all variables
ifeq ($(XILINX_OPENCL_DIR),)
$(error XILINK_OPENCL_DIR not set properly in Makefile.config)
endif
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif

PP=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/g++
CC=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/gcc

ifeq ("$(wildcard $(PP))","")
	$(error XILINK_OPENCL_DIR not set properly)
endif

MANAGER_DIR=../../manager

CFLAGS= -c -fPIC -std=c++0x -g
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/include \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(XILINX_OPENCL_DIR)/runtime/include/1_2
				 
LINK= -L$(MANAGER_DIR)/lib -lblaze \
			-L$(XILINX_OPENCL_DIR)/runtime/lib/x86_64 -lxilinxopencl \
			-lpthread -lm -ldl

DEPS= OpenCLPlatform.o \
			OpenCLBlock.o 

all: xil_ocl.so

xil_ocl.so: $(DEPS)
	$(PP) -shared -o $@ $(DEPS) $(LINK) 

%.o: %.cpp *.h Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf ../lib/xil_ocl.so
