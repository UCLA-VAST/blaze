ifeq ($(BLAZE_HOME),)	
$(error BLAZE_HOME is not set)
endif
ifeq ("$(wildcard $(BLAZE_HOME)/manager)","")
$(error BLAZE_HOME is not set correctly)
endif

# need to setup BLAZE_HOME correctly
include $(BLAZE_HOME)/Makefile.config

MANAGER_DIR = $(BLAZE_HOME)/manager

# check all variables
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR is not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR is not set properly in Makefile.config)
endif
ifeq ($(GLOG_DIR),)
$(error GLOG_DIR is not set properly in Makefile.config)
endif
ifeq ($(GTEST_DIR),)
$(error GTEST_DIR is not set properly in Makefile.config)
endif

# binaries
PP=g++
CC=gcc
CC=ar
RM=rm
ECHO=echo
MAKE=make

CFLAGS   := -c -fPIC -std=c++0x $(CFLAGS)

INCLUDES := -I$(BLAZE_HOME)/manager/include \
	    -I$(BOOST_DIR)/include \
	    -I$(PROTOBUF_DIR)/include \
	    -I$(GLOG_DIR)/include \
	    -I$(GTEST_DIR)/include \
	    $(INCLUDES)

COMPILE := $(CFLAGS) $(INCLUDES)

LINK    := -L$(BLAZE_HOME)/manager/lib -lblaze \
	   -L$(BOOST_DIR)/lib \
	   	-lboost_system \
	   	-lboost_thread \
	   	-lboost_iostreams \
	   	-lboost_filesystem \
	   	-lboost_regex \
	   -L$(PROTOBUF_DIR)/lib -lprotobuf \
	   -L$(GLOG_DIR)/lib -lglog \
	   -L$(GTEST_DIR)/build -lgtest \
	   -lpthread -lm -ldl \
	   $(LINK)

all: $(DST)

%.o: %.cpp
	$(PP) $(COMPILE) $< -o $@

clean:
	$(RM) -rf $(OBJS)
	$(RM) -rf $(DST)

.PHONY: all clean
