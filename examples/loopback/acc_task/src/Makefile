CPPFLAGS ?=
CXX      ?= g++
LDFLAGS  ?=

AAL_DIR=/curr/diwu/tools/aalsdk/4.1.7/aalsdk-4.1.7/build/install
MANAGER_DIR=/curr/diwu/prog/acc_runtime/manager
BOOST_DIR=/curr/diwu/tools/boost_1_55_0/install
PROTOBUF_DIR=/curr/diwu/tools/protobuf-2.5.0/build/install
XILINX_OPENCL_DIR=/space/Xilinx/SDAccel/2015.1

ifneq (,$(ndebug))
else
CPPFLAGS += -DENABLE_DEBUG=1
endif
ifneq (,$(nassert))
else
CPPFLAGS += -DENABLE_ASSERT=1
endif

CPPFLAGS += -fPIC -std=c++0x -m64 -g \
						-I$(AAL_DIR)/include \
				 		-I$(MANAGER_DIR)/include \
				 		-I$(BOOST_DIR)/include \
				 		-I$(PROTOBUF_DIR)/include \
				 		-I$(XILINX_OPENCL_DIR)/runtime/include/1_2

LDFLAGS  += -L$(AAL_DIR)/lib -Wl,-rpath-link \
						-Wl,$(AAL_DIR)/lib -Wl,-rpath \
						-Wl,$(AAL_DIR)/lib \
            -L$(AAL_DIR)/lib64 -Wl,-rpath-link \
						-Wl,$(AAL_DIR)/lib64 -Wl,-rpath \
						-Wl,$(AAL_DIR)/lib64

all: ../libNLB.so 

../libNLB.so: NLB.o
	$(CXX) -g -shared -o $@ $< $(LDFLAGS) -lOSAL -lAAS -lxlrt

NLB.o: NLB.cpp *.h Makefile
	$(CXX) $(CPPFLAGS) -D__AAL_USER__=1 -c -o $@ NLB.cpp

clean:
	$(RM) ../libNLB.so *.o

.PHONY:all clean
