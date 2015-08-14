# *******************************************************************************
# Vendor: Xilinx 
# Associated Filename: example_alphadata.tcl
# Purpose: Commands to construct the OpenCL C matrix multiply example
#                                                 
# *******************************************************************************
# Copyright (C) 2014 XILINX, Inc. 
# 
# This file contains confidential and proprietary information of Xilinx, Inc. and 
# is protected under U.S. and international copyright and other intellectual 
# property laws.
# 
# DISCLAIMER
# This disclaimer is not a license and does not grant any rights to the materials 
# distributed herewith. Except as otherwise provided in a valid license issued to 
# you by Xilinx, and to the maximum extent permitted by applicable law: 
# (1) THESE MATERIALS ARE MADE AVAILABLE "AS IS" AND WITH ALL FAULTS, AND XILINX 
# HEREBY DISCLAIMS ALL WARRANTIES AND CONDITIONS, EXPRESS, IMPLIED, OR STATUTORY, 
# INCLUDING BUT NOT LIMITED TO WARRANTIES OF MERCHANTABILITY, NON-INFRINGEMENT, OR 
# FITNESS FOR ANY PARTICULAR PURPOSE; and (2) Xilinx shall not be liable (whether 
# in contract or tort, including negligence, or under any other theory of 
# liability) for any loss or damage of any kind or nature related to, arising under 
# or in connection with these materials, including for any direct, or any indirect, 
# special, incidental, or consequential loss or damage (including loss of data, 
# profits, goodwill, or any type of loss or damage suffered as a result of any 
# action brought by a third party) even if such damage or loss was reasonably 
# foreseeable or Xilinx had been advised of the possibility of the same.
# 
# CRITICAL APPLICATIONS
# Xilinx products are not designed or intended to be fail-safe, or for use in any 
# application requiring fail-safe performance, such as life-support or safety 
# devices or systems, Class III medical devices, nuclear facilities, applications 
# related to the deployment of airbags, or any other applications that could lead 
# to death, personal injury, or severe property or environmental damage 
# (individually and collectively, "Critical Applications"). Customer assumes the 
# sole risk and liability of any use of Xilinx products in Critical Applications, 
# subject only to applicable laws and regulations governing limitations on product 
# liability. 
#
# THIS COPYRIGHT NOTICE AND DISCLAIMER MUST BE RETAINED AS PART OF THIS FILE AT 
# ALL TIMES.

#*******************************************************************************
# Define the solution for SDAccel
create_solution -name kmeans -dir . -force
add_device -vbnv xilinx:adm-pcie-7v3:1ddr:1.0

# Host Compiler Flags
set_property -name host_cflags -value "-g -Wall -D FPGA_DEVICE"  -objects [current_solution]

# Host Source Files
add_files "host.c"

# Kernel Definition
create_kernel run -type clc
add_files -kernel [get_kernels run] "kernel.cl"

# Define Binary Containers
create_opencl_binary kmeans
set_property region "OCL_REGION_0" [get_opencl_binary kmeans]
create_compute_unit -opencl_binary [get_opencl_binary kmeans] -kernel [get_kernels run] -name k1

#compile_host
# Compile the design for CPU based emulation
#compile_emulation -flow cpu -opencl_binary [get_opencl_binary kmeans]

# Run the compiled application in CPU based emulation mode
#run_emulation -flow cpu -args "kmeans.xclbin"

# Compile the application to run on the accelerator card
build_system

# Package the application binaries
package_system

