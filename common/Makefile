JAVA_OUT=../rddacc/src/main/java

all: task.proto
	protoc --cpp_out=../manager/src/task_msg --java_out=$(JAVA_OUT) task.proto
