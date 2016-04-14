include ../../Makefile.config

# check all variables
ifeq ($(XILINX_OPENCL_DIR),)
$(error XILINX_OPENCL_DIR not set properly in Makefile.config)
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
$(error XILINX_OPENCL_DIR not set properly)
endif

MANAGER_DIR	:=../../manager

CFLAGS 		:= -fPIC -std=c++0x

ifeq ($(NDEBUG),)
CFLAGS		:= $(CFLAGS) -g
else
CFLAGS 		:= $(CFLAGS) -O2 -DNDEBUG
endif

COMPILE		:= -c $(CFLAGS) \
		   -I$(MANAGER_DIR)/include \
		   -I$(MANAGER_DIR)/src \
		   -I$(BOOST_DIR)/include \
		   -I$(PROTOBUF_DIR)/include \
		   -I$(GLOG_DIR)/include \
		   -I$(XILINX_OPENCL_DIR)/runtime/include/1_2
				 
LINK		:= -L$(MANAGER_DIR)/lib -lblaze \
		   -L$(XILINX_OPENCL_DIR)/runtime/lib/x86_64 -lOpenCL \
		   -lpthread -lm -ldl

DEPS		:= OpenCLEnv.o \
		   OpenCLPlatform.o \
		   OpenCLBlock.o \
		   OpenCLQueueManager.o

DST		:= xlnx_opencl.so

all: $(DST)

package: 
	cp $(DST) ../../package/nam/lib

$(DST): $(DEPS)
	$(PP) -shared -o $@ $(DEPS) $(LINK) 

%.o: %.cpp *.h Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf $(DST)
