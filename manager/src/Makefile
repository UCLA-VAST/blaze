PROTO_DIR=./proto
BOOST_DIR=/curr/diwu/tools/boost_1_55_0/install
PROTOBUF_DIR=/curr/diwu/tools/protobuf-2.5.0/build/install
HADOOP_DIR=/curr/diwu/tools/hadoop/hadoop-2.5.2
XILINX_OPENCL_DIR=/space/Xilinx/SDAccel/2015.1

PP=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/g++
CC=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/gcc

CFLAGS= -c -fPIC -std=c++0x -g
COMPILE= $(CFLAGS) \
				 -I$(PROTO_DIR) \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(HADOOP_DIR)/include \
				 -I$(XILINX_OPENCL_DIR)/runtime/include/1_2

LINK= -L$(BOOST_DIR)/lib \
			-lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem \
			-L$(PROTOBUF_DIR)/lib -lprotobuf \
			-L$(HADOOP_DIR)/lib/native -lhdfs \
			-L$(JAVA_HOME)/jre/lib/amd64/server -ljvm \
			-L$(XILINX_OPENCL_DIR)/runtime/lib/x86_64 -lxilinxopencl \
			-lpthread -lm -ldl

DEPS= Comm.o \
			Context.o \
			BlockManager.o \
			QueueManager.o \
			TaskManager.o \
			Task_msg.o \
			Acc_conf.o

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
	rm -rf acc_manager
