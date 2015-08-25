#!/bin/bash

/curr/diwu/spark/1.3.1/bin/spark-submit --class LoopBack \
	--jars ${BLAZE_HOME}/target/BLAZE_RUNTIME-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/LoopBack-0.0.0.jar $@

