ifeq ($(FCS_RT_ROOT),)	
FCS_RT_ROOT=../../..
endif

PP=g++
CC=gcc
AR=ar

CFLAGS = -c -fPIC -std=c++0x -g
INCLUDES = -I$(FCS_RT_ROOT)/include \
				   -I$(FCS_RT_ROOT)/extern/boost_1_55_0/include \
				   -I$(FCS_RT_ROOT)/extern/googletools/include
COMPILE = $(CFLAGS) \
				  $(INCLUDES)

LINK = -L$(FCS_RT_ROOT)/nam/lib -lblaze \
			 -L$(FCS_RT_ROOT)/extern/boost_1_55_0/lib \
			 -lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem -lboost_regex \
			 -L$(FCS_RT_ROOT)/extern/googletools/lib -lprotobuf -lglog \
			 -lpthread -lm -ldl

all: map-bin reduce-bin

map-bin: map.o
	$(PP) -o $@ $< $(LINK)

reduce-bin: reduce.o
	$(PP) -o $@ $< $(LINK)

%.o: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean: 
	rm -rf *.o
	rm -rf map-bin
	rm -rf reduce-bin
