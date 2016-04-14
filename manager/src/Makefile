include ../../Makefile.config

# check all variables
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif
ifeq ($(GLOG_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif

# add support for flex license manage
FLMDIR 		:= /curr/peichen/work/test/license/production/falcon/build
FLMLIB 		:= -llmgr_trl -lcrvs -lsb -lnoact -llmgr_dongle_stub

CFLAGS 		:= -c -fPIC -std=c++0x

INCLUDES	:= -I../include \
		   -I$(BOOST_DIR)/include \
		   -I$(PROTOBUF_DIR)/include \
		   -I$(GLOG_DIR)/include 

LINK		:= -L../lib -lblaze \
		   -L$(BOOST_DIR)/lib \
		  	-lboost_system \
		   	-lboost_thread \
		   	-lboost_iostreams \
		   	-lboost_filesystem \
		   	-lboost_regex \
		   -L$(PROTOBUF_DIR)/lib -lprotobuf \
		   -L$(GLOG_DIR)/lib -lglog \
		   -lpthread -lm -Wl,--no-as-needed -ldl

ifeq ($(RELEASE),)
ifeq ($(NDEBUG),)
CFLAGS   	:= $(CFLAGS) -g
else
CFLAGS   	:= $(CFLAGS) -O2 -DNDEBUG
endif
else
CFLAGS   	:= $(CFLAGS) -O2 -DNDEBUG -DUSELICENSE
INCLUDES 	:= $(INCLUDES) -I$(FLMDIR)
LINK 	 	:= $(LINK) -L$(FLMDIR) $(FLMLIB) 
LMDEPS 	 	:= $(FLMDIR)/license.o \
		   $(FLMDIR)/lm_new.o 
endif

COMPILE = $(CFLAGS) $(INCLUDES)

DEPS		:= AccAgent.o \
		   Admin.o \
		   Block.o \
		   Client.o \
		   Common.o \
		   Platform.o \
		   Task.o \
		   TaskEnv.o \
		   BlockManager.o \
		   CommManager.o \
		   AppCommManager.o \
		   GAMCommManager.o \
		   PlatformManager.o \
		   TaskManager.o \
		   QueueManager.o \
		   acc_conf.pb.o \
		   task.pb.o \
		   msgGamNam.pb.o

all: 	../bin/acc_manager \
	../bin/acc_admin \
	../lib/libblaze.so

package: 
	cp ../bin/acc_manager ../../package/nam/bin/nam_daemon
	cp ../bin/acc_admin ../../package/nam/bin/admin_bin
	cp ../lib/libblaze.so ../../package/nam/lib
	cp Common.h ../../package/include
	cp Client.h ../../package/include
	cp TaskEnv.h ../../package/include

../lib/libblaze.so: $(DEPS)
	$(PP) -shared -o $@ $(DEPS)

../bin/acc_manager: ../lib/libblaze.so main.o $(LMDEPS)
	$(PP) -o $@ main.o $(LMDEPS) $(LINK)

../bin/acc_admin: ../lib/libblaze.so admin.o $(LMDEPS)
	$(PP) -o $@ admin.o $(LMDEPS) $(LINK)

%.o: proto/%.cc 
	$(PP) $(COMPILE) $< -o $@ 

%.o: %.cpp ../include/blaze/*.h proto/*.h Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf ../lib/libblaze.so
	rm -rf ../bin/acc_manager
