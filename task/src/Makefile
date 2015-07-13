CC=gcc
PP=g++

MANAGER_DIR=../../manager
BOOST_DIR=/curr/diwu/tools/boost_1_55_0/install
MKL_DIR=/curr/diwu/intel_mkl

CFLAGS= -shared -fPIC -std=c++0x 
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/include \
				 -I$(BOOST_DIR)/include \
				 -I$(MKL_DIR)/include

ACC = SimpleAddition \
			Circumference \
			Logistic

OBJS= $(addsuffix .so, $(addprefix ../lib/, $(ACC)))

all: $(OBJS)

../lib/%.so: %.cpp
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf $(OBJS)
