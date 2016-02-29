include ../Makefile.config

ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif

JAVA_OUT=../accrdd/src/main/java
CPP_OUT=../manager/src/proto
YARN_OUT=../hadoop-2.6.0-src/hadoop-yarn-project/hadoop-yarn/fcs-yarn-common/src/main/java
PYTHON_OUT=./python

all: task.proto acc_conf.proto msgGamNam.proto
	$(PROTOBUF_DIR)/bin/protoc --cpp_out=$(CPP_OUT) --java_out=$(JAVA_OUT) --python_out=$(PYTHON_OUT) task.proto
	$(PROTOBUF_DIR)/bin/protoc --cpp_out=$(CPP_OUT) --python_out=$(PYTHON_OUT) acc_conf.proto
	$(PROTOBUF_DIR)/bin/protoc --cpp_out=$(CPP_OUT) --java_out=$(YARN_OUT) msgGamNam.proto
