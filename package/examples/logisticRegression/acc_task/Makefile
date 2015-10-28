ROOTDIR=../../..
BOOSTDIR=$(ROOTDIR)/boost_1_55_0

PP=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/g++
CC=$(XILINX_OPENCL_DIR)/lnx64/tools/gcc/bin/gcc

CFLAGS= -shared -fPIC -std=c++0x -m64
COMPILE= $(CFLAGS) \
				 -I$(ROOTDIR)/include \
				 -I$(BOOSTDIR)/include \
				 -I$(XILINX_OPENCL_DIR)/runtime/include/1_2

ACC = LogisticGradientWithLoss_base \
			LogisticGradientWithLoss

OBJS= $(addsuffix .so, $(ACC))

all: $(OBJS)

%.so: %.cpp Makefile
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf $(OBJS)
