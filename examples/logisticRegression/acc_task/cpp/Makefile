ifeq ($(BLAZE_HOME),)
$(error BLAZE_HOME not set)
endif

MANAGER_DIR=$(BLAZE_HOME)/manager

PP=g++
CC=gcc

CFLAGS= -shared -fPIC -std=c++0x -m64 -g 
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/include 

ACC = LogisticGradientWithLoss_base

OBJS= $(addsuffix .so, $(addprefix ../, $(ACC)))

all: $(OBJS)

../%.so: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf $(OBJS)
