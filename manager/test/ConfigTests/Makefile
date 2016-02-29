include ../../../Makefile.config

MANAGER_DIR=../..

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


CFLAGS= -c -fPIC -std=c++0x -g
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/src \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(GLOG_DIR)/include 

LINK= -L$(MANAGER_DIR)/lib -lblaze \
			-L$(BOOST_DIR)/lib \
			-lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem -lboost_regex \
			-L$(PROTOBUF_DIR)/lib -lprotobuf \
			-L$(GLOG_DIR)/lib -lglog \
			-L$(GTEST_DIR)/build -lgtest \
			-lpthread -lm -ldl

all: admin

admin: admin.o
	$(PP) -o $@ admin.o $(LINK) 

%.o: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm admin

