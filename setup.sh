export JAVA_HOME=/curr/diwu/tools/jdk
export BOOST_DIR=/curr/diwu/tools/boost_1_55_0/install
export PROTOBUF_DIR=/curr/diwu/tools/protobuf-2.5.0/build/install
export HADOOP_DIR=/curr/diwu/tools/hadoop/hadoop-2.5.2
export MKL_DIR=/curr/diwu/intel_mkl

export BLAZE_HOME=`pwd`/accrdd

export LD_LIBRARY_PATH=$BOOST_DIR/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$PROTOBUF_DIR/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$HADOOP_DIR/lib/native:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$MKL_DIR/lib/intel64:$LD_LIBRARY_PATH

export LM_LICENSE_FILE=1800@fcs:2100@fcs:$LM_LICENSE_FILE
export XILINX_OPENCL=/home/Xilinx/SDAccel_2015.1.3/SDAccel/2015.1

source $XILINX_OPENCL/settings64.sh
export LD_LIBRARY_PATH=$XILINX_OPENCL/runtime/lib/x86_64:$LD_LIBRARY_PATH
export XCL_PLATFORM=xilinx_adm-pcie-7v3_1ddr_1_0
