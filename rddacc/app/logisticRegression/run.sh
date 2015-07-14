#!/bin/bash

spark-submit --class LogisticRegression \
	--jars ${ACC_HOME}/target/ACC_RUNTIME-1.0-SNAPSHOT.jar \
	--driver-memory 2G \
	--executor-memory 8G \
	--master local[*] \
	target/logisticRegression-0.0.0.jar /curr/diwu/prog/logistic/data/train_data.txt 5

