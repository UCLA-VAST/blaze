ifeq ($(FCS_RT_ROOT),)	
FCS_RT_ROOT=../../..
endif
ifeq ("$(wildcard $(FCS_RT_ROOT)/nam/bin/nam_daemon)","")
$(error FCS_RT_ROOT seems incorrect, please set it to the root directory of FCS_RUNTIME)
endif

PP=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/g++
CC=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/gcc

ifeq ("$(wildcard $(PP))","")
$(error XILINX_OPENCL_DIR is not properly defined)
endif

CFLAGS	:= -shared -fPIC -std=c++0x -m64 -g 
COMPILE	:= $(CFLAGS) \
	   -I$(FCS_RT_ROOT)/include \
	   -I$(FCS_RT_ROOT)/extern/boost_1_55_0/include \
	   -I$(FCS_RT_ROOT)/extern/googletools/include \
	   -I$(XILINX_OPENCL_DIR)/runtime/include/1_2

LINK	:= -L$(XILINX_OPENCL_DIR)/runtime/lib/x86_64 -lOpenCL

ACC 	:= LogisticGradientWithLoss

OBJS	:= $(addsuffix .so, $(ACC))

all: $(OBJS)

%.so: %.cpp
	$(PP) $(COMPILE) $(LINK) $< -o $@

clean:
	rm -rf $(OBJS)
