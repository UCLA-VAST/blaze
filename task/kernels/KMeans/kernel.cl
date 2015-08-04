#pragma OPENCL EXTENSION cl_khr_fp64 : enable

__kernel void run(
  __global double *data, 
  __global int *num_data, 
	__global double *centers, 
	__global int *num_centers,
	__global int *dims, 
	__global int *closest_centers) {

	int i = get_global_id(0);
	int nthreads = get_global_size(0);

	for (; i < *num_data; i += nthreads) {
		int closest_center = -1;
		double closest_center_dis = 0;
		for (int j = 0; j < *num_centers; ++j) {
			double dis = 0;
			for (int d = 0; d < *dims; ++d) {
				dis += 	(centers[j * (*dims) + d] - data[i * (*dims) + d]) * 
								(centers[j * (*dims) + d] - data[i * (*dims) + d]);
			}
			if (dis < closest_center_dis || closest_center == -1) {
				closest_center = j;
				closest_center_dis = dis;
			}
		}
		closest_centers[i] = closest_center;
	}
}
