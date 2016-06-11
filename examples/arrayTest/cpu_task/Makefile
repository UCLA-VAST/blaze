MANAGER_DIR=../../../manager
include ../../../Makefile.config

# check all variables
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif
ifeq ($(GLOG_DIR),)
$(error GLOG_DIR not set properly in Makefile.config)
endif

CFLAGS	:= -shared -fPIC -std=c++0x -m64
COMPILE	:= $(CFLAGS) \
	   -I$(MANAGER_DIR)/include \
	   -I$(BOOST_DIR)/include \
	   -I$(PROTOBUF_DIR)/include \
	   -I$(GFLAGS_DIR)/include \
	   -I$(GLOG_DIR)/include 

ACC = ArrayTest 

OBJS= $(addsuffix .so, $(ACC))

all: $(OBJS)

%.so: %.cpp 
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf $(OBJS)
