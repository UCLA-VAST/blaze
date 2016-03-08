#!/bin/bash

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOTDIR=$SCRIPT_DIR/../
ROOTDIR=$( cd "$ROOTDIR" && pwd )

if [ ! -f conf ]; then
  echo "No conf file found"
  exit -1
fi
cp conf conf_bak
sed -i "s/\[FCSROOT\]/$(echo $ROOTDIR | sed -e 's/[\/&]/\\&/g')/g" conf
sed -i "s/\[PWD\]/$(echo $PWD | sed -e 's/[\/&]/\\&/g')/g" conf

LD_LIBRARY_PATH=$ROOTDIR/nam/lib:$LD_LIBRARY_PATH
LD_LIBRARY_PATH=$ROOTDIR/extern/boost_1_55_0/lib:$LD_LIBRARY_PATH
LD_LIBRARY_PATH=$ROOTDIR/extern/googletools/lib:$LD_LIBRARY_PATH

# run command
$ROOTDIR/nam/bin/admin_bin d conf

mv conf_bak conf
