create_solution -name bit -dir . -force
add_device -vbnv xilinx:adm-pcie-ku3:2ddr:2.1

# Kernel Definition
create_kernel top -type c
add_files -kernel [get_kernels top] "memcpy_kernel.cpp"

# Define Binary Containers
create_opencl_binary top
set_property region "OCL_REGION_0" [get_opencl_binary top]
create_compute_unit -opencl_binary [get_opencl_binary top] -kernel [get_kernels top] -name k1

# Compile the application to run on the accelerator card
build_system

