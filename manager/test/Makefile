CFLAGS := -g
OBJS   := src/main.o \
					src/AppClients.o \
					src/BlockTests.o \
					src/ClientTests.o \
					src/ConfigTests.o

DST  	 := bin/test_app

include common.mk

test: 
	LD_LIBRARY_PATH=$(BLAZE_HOME)/manager/lib:$(LD_LIBRARY_PATH) \
	GLOG_log_dir="logs" \
	GLOG_stderrthreshold=3 \
	GLOG_v=3 \
	./$(DST)

tasks:
	$(MAKE) -C tasks

$(DST): $(OBJS)
	$(PP) -o $@ $(OBJS) $(LINK) 

.PHONY: test tasks
