PP=g++
CC=gcc

ifeq ($(FCS_RT_ROOT),)	
FCS_RT_ROOT=../../..
endif
ifeq ("$(wildcard $(FCS_RT_ROOT)/nam/bin/nam_daemon)","")
$(error FCS_RT_ROOT seems incorrect, please set it to the root directory of FCS_RUNTIME)
endif

CFLAGS	:= -c -fPIC -std=c++0x
COMPILE	:= $(CFLAGS) \
	   -I$(FCS_RT_ROOT)/include \
	   -I$(FCS_RT_ROOT)/extern/boost_1_55_0/include \
	   -I$(FCS_RT_ROOT)/extern/googletools/include
	
LINK	:= -L$(FCS_RT_ROOT)/nam/lib -lblaze \
	   -L$(FCS_RT_ROOT)/extern/boost_1_55_0/lib \
	   -lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem -lboost_regex \
	   -L$(FCS_RT_ROOT)/extern/googletools/lib -lprotobuf -lglog \
	   -lpthread -lm -ldl

all: app

app: main.o 
	$(PP) -o $@ main.o $(LINK) 

%.o: %.cpp
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf app 
