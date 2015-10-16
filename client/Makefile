include ../Makefile.config

MANAGER_DIR=../manager

PP=g++

CFLAGS= -c -fPIC -std=c++0x -g
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/src \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include
	
LINK= -L$(MANAGER_DIR)/lib -lblaze \
			-L$(BOOST_DIR)/lib \
			-lboost_system -lboost_thread -lboost_iostreams -lboost_filesystem \
			-L$(PROTOBUF_DIR)/lib -lprotobuf \
			-lpthread -lm -ldl

all: app

app: main.o 
	$(PP) -o $@ main.o $(LINK) 

%.o: %.cpp *.h Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf *.o
	rm -rf app 
