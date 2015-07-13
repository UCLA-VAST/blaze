#!/bin/bash

spark-submit --class LogisticRegressionApp \
	--jars ${ACC_HOME}/target/ACC_RUNTIME-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/logisticRegressionApp-0.0.0.jar


