#!/bin/bash

/curr/diwu/spark/1.3.1/bin/spark-submit --class LogisticRegression \
	--driver-memory 4G \
	--executor-memory 4G \
	--jars ${BLAZE_HOME}/target/BLAZE_RUNTIME-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/logisticRegression-0.0.0.jar /curr/diwu/prog/logistic/data/train_data.txt $@


