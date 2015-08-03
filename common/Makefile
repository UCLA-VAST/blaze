JAVA_OUT=../rddacc/src/main/java
CPP_OUT=../manager/src/proto

all: task.proto acc_conf.proto
	protoc --cpp_out=$(CPP_OUT) --java_out=$(JAVA_OUT) task.proto
	protoc --cpp_out=$(CPP_OUT) acc_conf.proto
