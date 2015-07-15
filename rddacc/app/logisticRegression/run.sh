#!/bin/bash

spark-submit --class LogisticRegression \
	--jars ${ACC_HOME}/target/ACC_RUNTIME-1.0-SNAPSHOT.jar \
	--driver-memory 64G \
	--executor-memory 64G \
	--master local[*] \
	target/logisticRegression-0.0.0.jar /curr/diwu/prog/logistic/data/train_data.txt 20

