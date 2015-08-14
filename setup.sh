export ACC_HOME=`pwd`/rddacc
export LD_LIBRARY_PATH=/curr/diwu/tools/hadoop/hadoop-2.5.2/lib/native:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/curr/diwu/intel_mkl/lib/intel64:/lib/intel64:$LD_LIBRARY_PATH

VERSION=2015.1
export LM_LICENSE_FILE=/space/Xilinx/SDAccel/Xilinx.lic:$LM_LICENSE_FILE
export XILINX_OPENCL=/space/Xilinx/SDAccel/$VERSION

source $XILINX_OPENCL/settings64.sh
export LD_LIBRARY_PATH=$XILINX_OPENCL/runtime/lib/x86_64:$LD_LIBRARY_PATH
export XCL_PLATFORM=xilinx_adm-pcie-7v3_1ddr_1_0
