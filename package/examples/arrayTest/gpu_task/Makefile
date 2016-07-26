PP=g++
CC=gcc

ifeq ($(FCS_RT_ROOT),)	
FCS_RT_ROOT=../../..
endif
ifeq ("$(wildcard $(FCS_RT_ROOT)/nam/bin/nam_daemon)","")
$(error FCS_RT_ROOT seems incorrect, please set it to the root directory of FCS_RUNTIME)
endif

CUDA_DIR	:=/usr/local/cuda

CFLAGS		:= -shared -fPIC -std=c++0x -m64
COMPILE		:= $(CFLAGS) \
						 -I$(FCS_RT_ROOT)/include \
						 -I$(FCS_RT_ROOT)/extern/boost_1_55_0/include \
						 -I$(FCS_RT_ROOT)/extern/googletools/include \
						 -I$(CUDA_DIR)/include

LINK			:= -L$(CUDA_DIR)/lib64 -lOpenCL

ACC 			:= ArrayTest

OBJS			:= $(addsuffix .so, $(ACC))

all: $(OBJS)

%.so: %.cpp
	$(PP) $(COMPILE) $(LINK) $< -o $@

clean:
	rm -rf $(OBJS)
