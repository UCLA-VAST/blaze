include ../../Makefile.config

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
ifeq ($(JAVA_HOME),)
$(error JAVA_HOME not set properly in Makefile.config)
endif

PP=g++
CC=gcc
AR=ar

CFLAGS= -c -fPIC -std=c++0x -g
COMPILE= $(CFLAGS) \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(HADOOP_DIR)/include 

LINK= -L../lib -lblaze \
			-L$(BOOST_DIR)/lib \
			-lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem \
			-L$(PROTOBUF_DIR)/lib -lprotobuf \
			-L$(HADOOP_DIR)/lib/native -lhdfs \
			-L$(JAVA_HOME)/jre/lib/amd64/server -ljvm \
			-lpthread -lm -ldl

DEPS= CommManager.o \
			AppCommManager.o \
			GAMCommManager.o \
			PlatformManager.o \
			Block.o \
			BlockManager.o \
			QueueManager.o \
			Task.o \
			TaskManager.o \
			task.pb.o \
			acc_conf.pb.o \
			msgGamNam.pb.o

all: ../bin/acc_manager ../lib/libblaze.so

../lib/libblaze.so: $(DEPS)
	$(PP) -shared -o $@ $(DEPS)

../bin/acc_manager: ../lib/libblaze.so main.o
	$(PP) -o $@ main.o $(LINK) 

task.pb.o: proto/task.pb.cc
	$(PP) $(COMPILE) $< -o $@ 

acc_conf.pb.o: proto/acc_conf.pb.cc
	$(PP) $(COMPILE) $< -o $@ 

msgGamNam.pb.o: proto/msgGamNam.pb.cc
	$(PP) $(COMPILE) $< -o $@ 

%.o: %.cpp *.h proto/*.h
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf ../bin/acc_manager
