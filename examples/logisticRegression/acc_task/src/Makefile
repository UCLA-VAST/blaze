MANAGER_DIR=$(BLAZE_HOME)/../manager
include ../../../../Makefile.config
MKL_DIR=

PP=g++
CC=gcc

CFLAGS= -shared -fPIC -std=c++0x -m64 -g
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/include \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(HADOOP_DIR)/include \
				 -I$(MKL_DIR)/include 

LINK= -L${MKL_DIR}/lib/intel64 \
			-lmkl_intel_lp64 -lmkl_core -lmkl_sequential \
			-lpthread -lm

ACC = Logistic 

OBJS= $(addsuffix .so, $(addprefix ../, $(ACC)))

all: $(OBJS)

../%.so: %.cpp
	$(PP) $(COMPILE) $(LINK) $< -o $@

clean:
	rm -rf $(OBJS)
