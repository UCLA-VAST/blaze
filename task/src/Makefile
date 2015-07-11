CC=gcc
PP=g++

MANAGER_DIR=../../manager
BOOST_DIR=/curr/diwu/tools/boost_1_55_0/install

CFLAGS= -shared -fPIC -std=c++0x 
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/include \
				 -I$(BOOST_DIR)/include 
DSTDIR=../lib

ACC=SimpleAddition
OBJS= $(DSTDIR)/$(ACC).so 

all: $(OBJS)

$(DSTDIR)/%.so: %.cpp
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf $(OBJS)
