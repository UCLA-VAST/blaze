#!/bin/bash

spark-submit --class LogisticRegression \
	--jars ${BLAZE_HOME}/target/BLAZE_RUNTIME-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/logisticRegression-0.0.0.jar ../train_data.txt 10 3

#	--driver-memory 1G \
#	--executor-memory 1G \

