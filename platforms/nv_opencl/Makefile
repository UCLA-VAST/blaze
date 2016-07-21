include ../../Makefile.config

# check all variables
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif

PP=g++
CC=gcc

MANAGER_DIR=../../manager
CUDA_DIR=/usr/local/cuda

CFLAGS 	:= -c -fPIC -std=c++0x

ifeq ($(NDEBUG),)
CFLAGS 	:= $(CFLAGS) -g
else
CFLAGS 	:= $(CFLAGS) -O2 -DNDEBUG
endif

COMPILE	:= $(CFLAGS) -I./include \
	   -I$(MANAGER_DIR)/include \
	   -I$(CUDA_DIR)/include \
	   -I$(BOOST_DIR)/include \
	   -I$(PROTOBUF_DIR)/include \
	   -I$(GLOG_DIR)/include
				 
LINK	:= -L$(MANAGER_DIR)/lib -lblaze \
	   -L$(CUDA_DIR)/lib64 -lOpenCL \
	   -lpthread -lm -ldl

DEPS	:= src/OpenCLPlatform.o \
	   src/OpenCLTaskEnv.o \
	   src/OpenCLBlock.o \
	   src/OpenCLQueueManager.o

PKG_DIR	:= ../../package

INC_LIST:= ./include/blaze/nv_opencl/OpenCLEnv.h \
	   ./include/blaze/nv_opencl/OpenCLCommon.h 

all: nv_opencl.so

package:
	for file in $(INC_LIST); do \
		cp $$file $(PKG_DIR)/include/blaze/nv_opencl; \
	done
	cp nv_opencl.so $(PKG_DIR)/nam/lib

release:
	$(MAKE) NDEBUG=1

nv_opencl.so: $(DEPS)
	$(PP) -shared -o $@ $(DEPS) $(LINK) 

src/%.o: src/%.cpp include/blaze/nv_opencl/*.h
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf src/*.o
	rm -rf nv_opencl.so
