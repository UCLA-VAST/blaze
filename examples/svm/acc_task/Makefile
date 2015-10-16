ifeq ($(BLAZE_HOME),)
$(error BLAZE_HOME not set)
endif
ifeq ("$(wildcard $(BLAZE_HOME)/Makefile.config)","")
$(error BLAZE_HOME not set properly in environment)
endif

MANAGER_DIR=$(BLAZE_HOME)/manager
include $(BLAZE_HOME)/Makefile.config

# check all variables
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif
PP=g++
CC=gcc

CFLAGS= -shared -fPIC -std=c++0x -m64 -g 
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/include \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include 

ACC = HingeGradient_base

OBJS= $(addsuffix .so, $(addprefix ../, $(ACC)))

all: $(OBJS)

../%.so: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf $(OBJS)
