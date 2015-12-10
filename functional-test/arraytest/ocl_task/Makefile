ifeq ($(BLAZE_HOME),)
$(error BLAZE_HOME not set)
endif
ifeq ("$(wildcard $(BLAZE_HOME)/Makefile.config)","")
$(error BLAZE_HOME not set properly in environment)
endif

MANAGER_DIR=$(BLAZE_HOME)/manager
PLATFORM_DIR=$(BLAZE_HOME)/platforms/gpu-ocl
include $(BLAZE_HOME)/Makefile.config

PP=g++
CC=gcc

CFLAGS= -shared -fPIC -std=c++0x -m64 -g 
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/include \
				 -I$(MANAGER_DIR)/src \
				 -I$(PLATFORM_DIR) \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(HADOOP_DIR)/include \
				 -I$(CUDA_DIR)/include

ACC = ArrayTest

OBJS= $(addsuffix .so, $(ACC))

all: $(OBJS)

%.so: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf $(OBJS)
