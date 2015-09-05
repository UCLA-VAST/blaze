MANAGER_DIR=../../../../manager
PLATFORM_DIR=../../../../platforms/xil_ocl
include ../../../../Makefile.config

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

CFLAGS= -shared -fPIC -std=c++0x -m64 -g 
COMPILE= $(CFLAGS) \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(MANAGER_DIR)/include \
				 -I$(PLATFORM_DIR) \
				 -I$(XILINX_OPENCL_DIR)/runtime/include/1_2

LINK= -L$(XILINX_OPENCL_DIR)/runtime/lib/x86_64 -lxilinxopencl 

ACC = Logistic 

OBJS= $(addsuffix .so, $(addprefix ../, $(ACC)))

all: $(OBJS)

../%.so: %.cpp Makefile
	$(PP) $(COMPILE) $(LINK) $< -o $@

clean:
	rm -rf $(OBJS)
