include ../../Makefile.config

XILINX_OPENCL_DIR=/space/Xilinx/SDAccel/2015.1
MANAGER_DIR=../../manager

PP=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/g++
CC=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/gcc

CFLAGS= -c -fPIC -std=c++0x -g
COMPILE= $(CFLAGS) \
				 -I../../manager/src \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(XILINX_OPENCL_DIR)/runtime/include/1_2
				 
LINK= -L$(MANAGER_DIR)/lib -lblaze \
			-L$(BOOST_DIR)/lib \
			-lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem \
			-L$(XILINX_OPENCL_DIR)/runtime/lib/x86_64 -lxilinxopencl \
			-lpthread -lm -ldl

DEPS= OpenCLPlatform.o \
			OpenCLBlock.o 

all: ../lib/xil_ocl.so

../lib/xil_ocl.so: $(DEPS)
	$(PP) -shared -o $@ $(DEPS) $(LINK) 

%.o: %.cpp *.h Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf ../lib/xil_ocl.so
