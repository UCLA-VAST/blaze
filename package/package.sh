#!/bin/bash

mkdir fcs_runtime
rsync -av boost_1_55_0.tar.gz fcs_runtime
rsync -av googletools.tar.gz fcs_runtime
rsync -av spark-1.4.0-bin-fcs.tar.gz fcs_runtime
rsync -av hadoop-2.6.0-bin-fcs.tar.gz fcs_runtime
rsync -av fcslm-lin64.tar.gz fcs_runtime
rsync -arv --exclude=".*" bin fcs_runtime
rsync -arv --exclude=".*" nam fcs_runtime
rsync -arv --exclude=".*" lib fcs_runtime
rsync -arv --exclude=".*" docs fcs_runtime
rsync -arv --exclude=".*" examples fcs_runtime
rsync -arv --exclude=".*" include fcs_runtime
rsync -arv --exclude=".*" install.sh fcs_runtime
rsync -arv --exclude=".*" README.md fcs_runtime

tar zcf fcs_runtime.tar.gz fcs_runtime
rm -rf fcs_runtime
