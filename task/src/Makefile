CC=gcc
PP=g++

MANAGER_DIR=../../manager
BOOST_DIR=/curr/diwu/tools/boost_1_55_0/install
MKL_DIR=/curr/diwu/intel_mkl
HADOOP_DIR=/curr/diwu/tools/hadoop/hadoop-2.5.2

CFLAGS= -shared -fPIC -std=c++0x -m64 -O3
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/include \
				 -I$(BOOST_DIR)/include \
				 -I$(HADOOP_DIR)/include \
				 -I$(MKL_DIR)/include
#LINK= -Wl,--start-group \
#			${MKL_DIR}/lib/intel64/libmkl_intel_lp64.a \
#			${MKL_DIR}/lib/intel64/libmkl_core.a \
#			${MKL_DIR}/lib/intel64/libmkl_sequential.a \
#			-Wl,--end-group -lpthread -lm
LINK= -L${MKL_DIR}/lib/intel64 \
			-lmkl_intel_lp64 -lmkl_core -lmkl_sequential \
			-lpthread -lm

ACC = SimpleAddition \
			Circumference \
			Logistic

OBJS= $(addsuffix .so, $(addprefix ../lib/, $(ACC)))

all: $(OBJS)

../lib/%.so: %.cpp
	$(PP) $(COMPILE) $(LINK) $< -o $@

clean:
	rm -rf $(OBJS)
