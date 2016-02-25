include ../../../../Makefile.config

PP=g++
CC=gcc

MANAGER_DIR=../../../

CFLAGS= -shared -fPIC -std=c++0x -m64
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/src

ACC = ArrayTest 

OBJS= $(addsuffix .so, $(ACC))

all: $(OBJS)

%.so: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf $(OBJS)
