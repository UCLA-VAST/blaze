ifeq ($(BLAZE_HOME),)	
$(error BLAZE_HOME is not set)
endif
ifeq ("$(wildcard $(BLAZE_HOME)/manager)","")
$(error BLAZE_HOME is not set correctly)
endif

CFLAGS = -O2 
include $(BLAZE_HOME)/manager/test/common.mk

%.so: %.o
	$(PP) -shared $< -o $@ 

