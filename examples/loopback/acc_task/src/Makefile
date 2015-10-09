include ../../../../Makefile.config
MANAGER_DIR=../../../../manager

CPPFLAGS ?=
CXX      ?= g++
LDFLAGS  ?=

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
				 		-I$(PROTOBUF_DIR)/include 

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
