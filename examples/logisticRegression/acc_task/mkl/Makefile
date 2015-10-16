MANAGER_DIR=../../../../manager
include ../../../../Makefile.config

# check all variables
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif
ifeq ($(MKL_DIR),)
$(error MKL_DIR not set properly in Makefile.config)
endif
ifeq ($(HADOOP_DIR),)
$(error HADOOP_DIR not set properly in Makefile.config)
endif

PP=g++
CC=gcc

CFLAGS= -shared -fPIC -std=c++0x -m64 -O3 -DUSEMKL
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/include \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(HADOOP_DIR)/include \
				 -I$(MKL_DIR)/include \

LINK= -L${MKL_DIR}/lib/intel64 \
			-lmkl_intel_lp64 -lmkl_core -lmkl_sequential \
			-lpthread -lm

ACC = Logistic  \
			LogisticGradientWithLoss

OBJS= $(addsuffix .so, $(addprefix ../, $(ACC)))

all: $(OBJS)

../%.so: %.cpp Makefile
	$(PP) $(COMPILE) $(LINK) $< -o $@

clean:
	rm -rf $(OBJS)
