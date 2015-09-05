MANAGER_DIR=../../../../manager
PLATFORM_DIR=../../../../platforms/xil_ocl
include ../../../../Makefile.config

PP=g++
CC=gcc

CFLAGS= -fPIC -std=c++0x -m64 -g 
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/src \
				 -I$(MANAGER_DIR)/src/proto \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include 

LINK= -L$(BOOST_DIR)/lib \
			-lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem \
			-L$(PROTOBUF_DIR)/lib -lprotobuf \
			-lpthread -lm -ldl

DEPS= $(MANAGER_DIR)/src/PlatformManager.o \
			$(MANAGER_DIR)/src/Block.o \
			$(MANAGER_DIR)/src/BlockManager.o \
			$(MANAGER_DIR)/src/QueueManager.o \
			$(MANAGER_DIR)/src/Task.o \
			$(MANAGER_DIR)/src/TaskManager.o \
			$(MANAGER_DIR)/src/Acc_conf.o 

all: run_test

run_test: LogisticGradient.o
	$(PP) -o $@ $(DEPS) LogisticGradient.o $(LINK) 

%.o: %.cpp Makefile
	$(PP) -c $(COMPILE) $< -o $@

clean:
	rm -rf run_test
	rm -rf *.o
