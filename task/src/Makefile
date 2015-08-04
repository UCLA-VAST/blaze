MANAGER_DIR=../../manager
BOOST_DIR=/curr/diwu/tools/boost_1_55_0/install
PROTOBUF_DIR=/curr/diwu/tools/protobuf-2.5.0/build/install
MKL_DIR=/curr/diwu/intel_mkl
HADOOP_DIR=/curr/diwu/tools/hadoop/hadoop-2.5.2
XILINX_OPENCL_DIR=/space/Xilinx/SDAccel/2015.1

PP=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/g++
CC=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/gcc

CFLAGS= -shared -fPIC -std=c++0x -O3 -m64
COMPILE= $(CFLAGS) \
				 -I$(MANAGER_DIR)/include \
				 -I$(BOOST_DIR)/include \
				 -I$(PROTOBUF_DIR)/include \
				 -I$(HADOOP_DIR)/include \
				 -I$(MKL_DIR)/include \
				 -I$(XILINX_OPENCL_DIR)/runtime/include/1_2

LINK= -L${MKL_DIR}/lib/intel64 \
			-L$(XILINX_OPENCL_DIR)/runtime/lib/x86_64 -lxilinxopencl \
			-lmkl_intel_lp64 -lmkl_core -lmkl_sequential \
			-lpthread -lm

ACC = SimpleAddition \
			Circumference \
			Logistic \
			Logistic_ocl \
			KMeans

OBJS= $(addsuffix .so, $(addprefix ../lib/, $(ACC)))

all: $(OBJS)

../lib/%.so: %.cpp
	$(PP) $(COMPILE) $(LINK) $< -o $@

clean:
	rm -rf $(OBJS)
