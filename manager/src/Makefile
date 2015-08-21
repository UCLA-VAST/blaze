include ../../Makefile.config

PROTO_DIR=./proto

PP=g++
CC=gcc

CFLAGS= -c -fPIC -std=c++0x -g
COMPILE= $(CFLAGS) \
				 -I$(PROTO_DIR) \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(HADOOP_DIR)/include 

LINK= -L$(BOOST_DIR)/lib \
			-lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem \
			-L$(PROTOBUF_DIR)/lib -lprotobuf \
			-L$(HADOOP_DIR)/lib/native -lhdfs \
			-L$(JAVA_HOME)/jre/lib/amd64/server -ljvm \
			-lpthread -lm -ldl

DEPS= Comm.o \
			Context.o \
			Block.o \
			BlockManager.o \
			QueueManager.o \
			Task.o \
			TaskManager.o \
			Task_msg.o \
			Acc_conf.o \

ifneq (,$(USE_OPENCL))
PP=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/g++
CC=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/gcc

LINK += -L$(XILINX_OPENCL_DIR)/runtime/lib/x86_64 -lxilinxopencl
COMPILE += -I$(XILINX_OPENCL_DIR)/runtime/include/1_2
DEPS += OpenCLBlock.o 
endif

all: ../bin/acc_manager

../bin/acc_manager: $(DEPS) main.o
	$(PP) -o $@ $(DEPS) main.o $(LINK) 

Task_msg.o: $(PROTO_DIR)/task.pb.cc
	$(PP) $(COMPILE) $< -o $@ 

Acc_conf.o: $(PROTO_DIR)/acc_conf.pb.cc
	$(PP) $(COMPILE) $< -o $@ 

%.o: %.cpp *.h
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf ../bin/acc_manager
