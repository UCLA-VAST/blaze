#pragma OPENCL EXTENSION cl_khr_fp64 : enable

__kernel __attribute__ ((reqd_work_group_size(1, 1, 1)))
void run(
  __global double *data, 
  __global int *num_data, 
	__global double *centers, 
	__global int *num_centers,
	__global int *dims, 
	__global int *output) {

//	int i = get_global_id(0);
//	int nthreads = get_global_size(0);
/*
	for (; i < num_data[0]; i += nthreads) {
		int closest_center = -1;
		double closest_center_dis = 0;

		for (int j = 0; j < num_centers[0]; ++j) {
			double dis = 0;

			for (int d = 0; d < dims[0]; ++d) {
				dis += 	(centers[j * dims[0] + d] - data[i * dims[0] + d]) * 
								(centers[j * dims[0] + d] - data[i * dims[0] + d]);
			}
			if (dis < closest_center_dis || closest_center == -1) {
				closest_center = j;
				closest_center_dis = dis;
			}
		}
		//output[i] = 1;//closest_center;
	}
*/
	output[0] = 2;
	return ;
}
