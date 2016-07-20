ifeq ($(FCS_RT_ROOT),)	
FCS_RT_ROOT=../../..
endif
ifeq ("$(wildcard $(FCS_RT_ROOT)/nam/bin/nam_daemon)","")
$(error FCS_RT_ROOT seems incorrect, please set it to the root directory of FCS_RUNTIME)
endif

ifeq ($(ALTERA_OPENCL_DIR),)
$(error ALTERA_OPENCL_DIR should be set for compilation)
endif

PP=g++
CFLAGS= -shared -fPIC -std=c++0x -m64
COMPILE= $(CFLAGS) \
				 -I$(FCS_RT_ROOT)/include \
				 -I$(FCS_RT_ROOT)/extern/boost_1_55_0/include \
				 -I$(FCS_RT_ROOT)/extern/googletools/include \
	 -I$(ALTERA_OPENCL_DIR)/include 
LINK= -L$(ALTERA_OPENCL_DIR)/linux64/lib -lalteracl

ACC = Memcpy

OBJS= $(addsuffix .so, $(ACC))

all: $(OBJS)

%.so: %.cpp
	$(PP) $(COMPILE) $(LINK) $< -o $@

clean:
	rm -rf $(OBJS)
