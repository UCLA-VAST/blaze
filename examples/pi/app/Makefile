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

PP=g++
CC=gcc
AR=ar

CFLAGS = -c -fPIC -std=c++0x -g
COMPILE := $(CFLAGS) \
				 	 -I$(MANAGER_DIR)/src \
				 	 -I$(BOOST_DIR)/include \
				 	 -I$(PROTOBUF_DIR)/include \
				 	 -I$(GLOG_DIR)/include

LINK		:= -L$(MANAGER_DIR)/lib -lblaze \
					 -L$(BOOST_DIR)/lib \
					 -lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem -lboost_regex \
					 -L$(PROTOBUF_DIR)/lib -lprotobuf \
					 -L$(GLOG_DIR)/lib -lglog \
					 -lpthread -lm -ldl

all: map-bin reduce-bin

map-bin: map.o
	$(PP) -o $@ $< $(LINK)

reduce-bin: reduce.o
	$(PP) -o $@ $< $(LINK)

%.o: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean: 
	rm -rf *.o
	rm -rf map-bin
	rm -rf reduce-bin
