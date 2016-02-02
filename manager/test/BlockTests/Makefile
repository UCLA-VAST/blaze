include ../../../Makefile.config

# check all variables
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
endif
ifeq ($(HADOOP_DIR),)
$(error HADOOP_DIR not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif
ifeq ($(GTEST_DIR),)
$(error GTEST_DIR not set properly in Makefile.config)
endif

PP=g++
CC=gcc
AR=ar

CFLAGS= -c -fPIC -std=c++0x -g
COMPILE= $(CFLAGS) \
				 -I$(BLAZE_HOME)/manager/src \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(GLOG_DIR)/include \
				 -I$(GTEST_DIR)/include 

LINK= -L$(BLAZE_HOME)/manager/lib -lblaze \
			-L$(BOOST_DIR)/lib \
			-lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem -lboost_regex \
			-L$(PROTOBUF_DIR)/lib -lprotobuf \
			-L$(GLOG_DIR)/lib -lglog \
			-L$(GTEST_DIR)/build -lgtest \
			-lpthread -lm -ldl

all: test

test: main.o
	$(PP) -o $@ main.o $(LINK) 

%.o: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm test

