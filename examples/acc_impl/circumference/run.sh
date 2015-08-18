#!/bin/bash

spark-submit --class CircumferenceApp \
	--jars ${BLAZE_HOME}/target/BLAZE_RUNTIME-1.0-SNAPSHOT.jar \
	--master local[*] \
	target/circumferenceApp-0.0.0.jar


