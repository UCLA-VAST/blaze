include ../Makefile.config

ifeq ($(PROTOBUF_DIR),)
$(error PROTOBUF_DIR not set properly in Makefile.config)
endif

JAVA_OUT=../accrdd/src/main/java
CPP_OUT=../manager/src/proto
YARN_OUT=../hadoop-2.6.0-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java

all: task.proto acc_conf.proto msgGamNam.proto
	$(PROTOBUF_DIR)/bin/protoc --cpp_out=$(CPP_OUT) --java_out=$(JAVA_OUT) task.proto
	$(PROTOBUF_DIR)/bin/protoc --cpp_out=$(CPP_OUT) acc_conf.proto
	$(PROTOBUF_DIR)/bin/protoc --cpp_out=$(CPP_OUT) --java_out=$(YARN_OUT) msgGamNam.proto
