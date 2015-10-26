#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [ -f $DIR/../conf/slaves ]; then
  slaves=`cat $DIR/../conf/slaves`
else
  slaves="localhost"
fi

for slave in $slaves; do
  ssh $slave source $DIR/start_nam.sh
done
