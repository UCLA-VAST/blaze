ifeq ($(BLAZE_HOME),)
$(error BLAZE_HOME not set)
endif
ifeq ("$(wildcard $(BLAZE_HOME)/Makefile.config)","")
$(error BLAZE_HOME not set properly in environment)
endif

MANAGER_DIR=$(BLAZE_HOME)/manager
PLATFORM_DIR=$(BLAZE_HOME)/platforms/altr_opencl
include $(BLAZE_HOME)/Makefile.config

CFLAGS= -shared -fPIC -std=c++0x -m64 -g
COMPILE= $(CFLAGS) \
	 -I$(MANAGER_DIR)/include \
	 -I$(PLATFORM_DIR) \
	 -I$(BOOST_DIR)/include \
	 -I$(PROTOBUF_DIR)/include \
	 -I$(HADOOP_DIR)/include \
	 -I$(ALTERA_OPENCL_DIR)/include 
LINK= -L$(ALTERA_OPENCL_DIR)/linux64/lib -lalteracl

ACC = memcpy

OBJS= $(addsuffix .so, $(ACC))

all: $(OBJS)

%.so: %.cpp Makefile
	$(PP) $(COMPILE) $(LINK) $< -o $@

clean:
	rm -rf $(OBJS)
