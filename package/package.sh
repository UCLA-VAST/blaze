#!/bin/bash

mkdir fcs_runtime
rsync -av *.tar.gz fcs_runtime
rsync -arv --exclude=".*" bin fcs_runtime
rsync -arv --exclude=".*" nam fcs_runtime
rsync -arv --exclude=".*" lib fcs_runtime
rsync -arv --exclude=".*" docs fcs_runtime
rsync -arv --exclude=".*" examples fcs_runtime
rsync -arv --exclude=".*" include fcs_runtime
rsync -arv --exclude=".*" setup.sh fcs_runtime
rsync -arv --exclude=".*" README fcs_runtime

tar zcf fcs_runtime.tar.gz fcs_runtime
rm -rf fcs_runtime
