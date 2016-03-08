#!/bin/bash

mkdir fcs_runtime
rsync -aLv boost_1_55_0.tar.gz fcs_runtime
rsync -aLv googletools.tar.gz fcs_runtime
rsync -aLv spark-1.5.1-bin-fcs.tar.gz fcs_runtime
rsync -aLv hadoop-2.6.0-bin-fcs.tar.gz fcs_runtime
rsync -aLv fcslm-lin64.tar.gz fcs_runtime
rsync -aLrv --exclude=".*" bin fcs_runtime
rsync -aLrv --exclude=".*" nam fcs_runtime
#rsync -aLrv --exclude=".*" lib fcs_runtime
#rsync -aLrv --exclude=".*" docs fcs_runtime
rsync -aLrv --exclude=".*" examples fcs_runtime
rsync -aLrv --exclude=".*" include fcs_runtime
rsync -aLrv --exclude=".*" install.sh fcs_runtime
rsync -aLrv --exclude=".*" setup-env.sh fcs_runtime
rsync -aLrv --exclude=".*" setup-env.csh fcs_runtime
rsync -aLrv --exclude=".*" README.md fcs_runtime

tar zcf fcs_runtime.tar.gz fcs_runtime
rm -rf fcs_runtime
