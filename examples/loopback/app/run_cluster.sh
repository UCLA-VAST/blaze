#!/bin/bash

spark-submit --class LoopBack \
	--jars ${BLAZE_HOME}/target/BLAZE_RUNTIME-1.0-SNAPSHOT.jar \
	--master spark://10.0.0.1:7077 \
	target/LoopBack-0.0.0.jar train_data.txt

