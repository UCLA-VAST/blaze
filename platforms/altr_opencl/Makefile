include ../../Makefile.config

# check all variables
ifeq ($(ALTERA_OPENCL_DIR),)
$(error ALTERA_OPENCL_DIR not set properly in Makefile.config)
endif
ifeq ($(ALTERA_OPENCL_BSP_DIR),)
$(error ALTERA_OPENCL_BSP_DIR not set properly in Makefile.config)
endif
ifeq ($(BOOST_DIR),)
$(error BOOST_DIR not set properly in Makefile.config)
endif
ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif

MANAGER_DIR	:=../../manager

CFLAGS 		:= -fPIC -std=c++0x

ifeq ($(NDEBUG),)
CFLAGS		:= $(CFLAGS) -g
else
CFLAGS 		:= $(CFLAGS) -O2 -DNDEBUG
endif

COMPILE		:= -c $(CFLAGS) -I./include \
		   -I$(MANAGER_DIR)/include \
		   -I$(BOOST_DIR)/include \
		   -I$(PROTOBUF_DIR)/include \
		   -I$(GLOG_DIR)/include \
		   -I$(GFLAGS_DIR)/include \
		   -I$(ALTERA_OPENCL_DIR)/include
				 
LINK		:= -L$(MANAGER_DIR)/lib -lblaze \
		   -L$(ALTERA_OPENCL_DIR)/linux64/lib -lalteracl \
		   -L$(ALTERA_OPENCL_BSP_DIR)/linux64/lib -laltera_a10_ref_mmd \
		   -lpthread -lm -ldl

DEPS		:= src/OpenCLEnv.o \
		   src/OpenCLPlatform.o \
		   src/OpenCLBlock.o \
		   src/OpenCLQueueManager.o

DST		:= altr_opencl.so

PKG_DIR		:= ../../package

INC_LIST	:= ./include/blaze/altr_opencl/OpenCLEnv.h \
		   ./include/blaze/altr_opencl/OpenCLCommon.h 
all: $(DST)

package: 
	for file in $(INC_LIST); do \
		cp $$file $(PKG_DIR)/include/blaze/altr_opencl; \
	done
	cp $(DST) $(PKG_DIR)/nam/lib

release:
	$(MAKE) NDEBUG=1

$(DST): $(DEPS)
	$(PP) -shared -o $@ $(DEPS) $(LINK) 

src/%.o: src/%.cpp include/blaze/altr_opencl/*.h
	$(PP) $(COMPILE) $< -o $@

clean:
	rm -rf src/*.o
	rm -rf $(DST)
