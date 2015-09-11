#!/bin/bash

spark-submit --class TestApp$1 \
	--jars ${BLAZE_HOME}/accrdd/target/blaze-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/test-0.0.0.jar


