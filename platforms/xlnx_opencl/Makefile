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

COMPILE		:= -c $(CFLAGS) -I./include \
		   -I$(MANAGER_DIR)/include \
		   -I$(BOOST_DIR)/include \
		   -I$(PROTOBUF_DIR)/include \
		   -I$(GLOG_DIR)/include \
		   -I$(XILINX_OPENCL_DIR)/runtime/include/1_2
				 
LINK		:= -L$(MANAGER_DIR)/lib -lblaze \
		   -L$(XILINX_OPENCL_DIR)/runtime/lib/x86_64 -lOpenCL \
		   -lpthread -lm -ldl

DEPS		:= src/OpenCLEnv.o \
		   src/OpenCLPlatform.o \
		   src/OpenCLBlock.o \
		   src/OpenCLQueueManager.o

DST		:= xlnx_opencl.so

PKG_DIR		:= ../../package

INC_LIST	:= ./include/blaze/xlnx_opencl/OpenCLEnv.h \
		   ./include/blaze/xlnx_opencl/OpenCLCommon.h 
all: $(DST)

package: 
	for file in $(INC_LIST); do \
		cp $$file $(PKG_DIR)/include/blaze/xlnx_opencl; \
	done
	cp $(DST) $(PKG_DIR)/nam/lib

release:
	$(MAKE) NDEBUG=1

$(DST): $(DEPS)
	$(PP) -shared -o $@ $(DEPS) $(LINK) 

src/%.o: src/%.cpp include/blaze/xlnx_opencl/*.h
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf src/*.o
	rm -rf $(DST)
