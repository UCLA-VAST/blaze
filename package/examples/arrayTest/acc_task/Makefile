PP=g++
CC=gcc

ROOTDIR=../../..

CFLAGS= -shared -fPIC -std=c++0x -m64
COMPILE= $(CFLAGS) \
				 -I$(ROOTDIR)/include \
				 -I$(ROOTDIR)/boost/include

ACC = ArrayTest 

OBJS= $(addsuffix .so, $(addprefix ../, $(ACC)))

all: $(OBJS)

../%.so: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf $(OBJS)
