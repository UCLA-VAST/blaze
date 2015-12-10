include ../../Makefile.config

# check all variables
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
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

# add support for flex license manage
FLMDIR = /curr/peichen/work/test/license/production/falcon/build
FLMLIB = -llmgr_trl -lcrvs -lsb -lnoact -llmgr_dongle_stub
XTRACFLAG = -DFLEX_INET6 -DRELEASE_VERSION -D__BITS64 -pipe \
						-DARCH_64 -DARCH_INTEL -DARCH_X86 -DARCH_X86_64 \
						-DOS_LINUX -pthread -DLM_INTERNAL -DFLEXLM_KITBUILD \
						-DFLEX_STATIC -DRELEASE_VERSION -DGPLATFORM=\\"x64_lsb\\" \
						-DLINUX -DLSB3_0 -D__FD_SETSIZE=65535 -DGLIBC \
						-DLINUX64 -DAMD64 -DREDHAT -DREL -DRHLINUX64 \
						-DPLATFORM_AMD64_RHLINUX 

CFLAGS = -c -fPIC -std=c++0x -g \
				 $(XTRACFLAG) -DNO_ACTIVATION_SUPPORT

INCLUDES = -I$(BOOST_DIR)/include \
				   -I$(PROTOBUF_DIR)/include \
				 	 -I$(GLOG_DIR)/include \
				   -I$(HADOOP_DIR)/include \
					 -I$(FLMDIR)

COMPILE = $(CFLAGS) \
				 $(INCLUDES)

LINK= -L../lib -lblaze \
			-L$(BOOST_DIR)/lib \
			-lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem \
			-L$(PROTOBUF_DIR)/lib -lprotobuf \
			-L$(GLOG_DIR)/lib -lglog \
			-L$(FLMDIR) $(FLMLIB) \
			-lpthread -lm \
			-Wl,--no-as-needed -ldl

DEPS= Common.o \
			Block.o \
			Task.o \
			CommManager.o \
			AppCommManager.o \
			GAMCommManager.o \
			Platform.o \
			PlatformManager.o \
			BlockManager.o \
			TaskManager.o \
			QueueManager.o \
			task.pb.o \
			acc_conf.pb.o \
			msgGamNam.pb.o

LMDEPS = $(FLMDIR)/license.o \
			 	 $(FLMDIR)/lm_new.o 

all: ../bin/acc_manager ../lib/libblaze.so

package: 
	cp ../bin/acc_manager ../../package/nam/bin/nam_daemon
	cp ../lib/libblaze.so ../../package/nam/lib

../lib/libblaze.so: $(DEPS)
	$(PP) -shared -o $@ $(DEPS)

../bin/acc_manager: ../lib/libblaze.so main.o $(LMDEPS)
	$(PP) -o $@ main.o $(LMDEPS) $(LINK)

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
