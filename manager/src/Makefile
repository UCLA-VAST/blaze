
CC=gcc
PP=g++

ACCMSG_DIR=./task_msg
BOOST_DIR=/curr/diwu/tools/boost_1_55_0/install
PROTOBUF_DIR=/curr/diwu/tools/protobuf-2.5.0/build/install

CFLAGS= -c -fPIC -std=c++0x
COMPILE= $(CFLAGS) \
				 -I$(ACCMSG_DIR) \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include 
LINK= -L$(BOOST_DIR)/lib -lboost_system -lboost_thread \
			-L$(PROTOBUF_DIR)/lib -lprotobuf \
			-lpthread -lm

DEPS= main.o \
			Comm.o \
			Task_msg.o 

all: ../bin/acc_manager

../bin/acc_manager: $(DEPS)
	$(PP) -o $@ $(DEPS) $(LINK) 

Task_msg.o: $(ACCMSG_DIR)/task.pb.cc
	$(PP) $(COMPILE) $< -o $@ 

%.o: %.cpp %.h
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf acc_manager
