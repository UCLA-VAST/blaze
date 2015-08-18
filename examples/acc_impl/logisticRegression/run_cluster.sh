#!/bin/bash

spark-submit --class LogisticRegression \
	--jars ${BLAZE_HOME}/target/BLAZE_RUNTIME-1.0-SNAPSHOT.jar \
	--master spark://10.0.0.1:7077 \
	target/logisticRegression-0.0.0.jar train_data.txt 10 3

