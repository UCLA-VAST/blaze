#!/bin/bash

spark-submit --class TestApp$1 \
	--jars ${ACC_HOME}/target/ACC_RUNTIME-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/testapp-0.0.0.jar


