MANAGER_DIR=../../../manager
include ../../../Makefile.config

# check all variables
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif

PP=g++

CFLAGS= -c -fPIC -std=c++0x -g
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/src \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include
	
LINK= -L$(MANAGER_DIR)/lib -lblaze \
			-L$(BOOST_DIR)/lib \
			-lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem -lboost_regex \
			-L$(PROTOBUF_DIR)/lib -lprotobuf \
			-L$(GLOG_DIR)/lib -lglog \
			-lpthread -lm -ldl

all: app

app: main.o 
	$(PP) -o $@ main.o $(LINK) 

%.o: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf app 
