#!/bin/bash

WORK_DIR=/user/$USER
if [ -z "$1" ]; then
  N=10001
else
  N=$1
fi

# create a temp file with N empty lines
TMP_FILE=temp-file-$N.txt
printf '\n%.0s' $(seq 1 $N) >> $TMP_FILE
hdfs dfs -copyFromLocal `pwd`/$TMP_FILE $WORK_DIR 

if [[ "$?" != "0" ]]; then
  echo "Task failed"
  exit 1
fi

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar \
    -files file:`pwd`/map-bin,file:`pwd`/reduce-bin,file:`pwd`/map.sh,file:`pwd`/reduce.sh \
    -D mapreduce.label=cpu \
    -input $WORK_DIR/$TMP_FILE \
    -output $WORK_DIR/output_pi \
    -mapper map.sh \
    -reducer reduce.sh &> hadoop.log

hdfs dfs -ls $WORK_DIR/output_pi/_SUCCESS &> /dev/null

if [[ "$?" != "0" ]]; then
  echo "Task failed"
else 
  ret=`hdfs dfs -cat $WORK_DIR/output_pi/* | head -n 1 | cut -f2`
  pi=`echo 4*$ret/$N | bc -l`
  echo "PI is estimated to be: $pi"
fi

# clean up
rm $TMP_FILE
hdfs dfs -rm $WORK_DIR/$TMP_FILE &> /dev/null
hdfs dfs -rm -r $WORK_DIR/output_pi &> /dev/null
