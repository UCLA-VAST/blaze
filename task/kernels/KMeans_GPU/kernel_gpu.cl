//#pragma OPENCL EXTENSION cl_khr_fp64 : enable

__kernel
void run(
  __global float *data,
	int num_data,
	__global float *centers,
	int num_centers,
	int dims,
	__global int *output) {

	int i = get_group_id(0);
	int nthreads = get_num_groups(0);

	int tid = get_local_id(0);
	int tdim = get_local_size(0);

	__local float tmp[1024];
	__local float l_centers[4096];

	if (tid < dims) {
		for (int j = 0; j < num_centers; ++j)
			l_centers[j * dims + tid] = centers[j * dims + tid];
	}
	barrier(CLK_LOCAL_MEM_FENCE);

//	for (; i < num_data; i += nthreads) {
		__local int closest_center;
		__local float closest_center_dis;

		if (tid == 0) {
			closest_center = -1;
			closest_center_dis = 0;
		}

		float l_data = data[i * dims + tid];
		for (int j = 0; j < num_centers; ++j) {

			if (tid < dims) {
				tmp[tid] = (l_centers[j * dims + tid] - l_data) * 
									 (l_centers[j * dims + tid] - l_data);
//				if (get_group_id(0) == 0)
//					printf("%d: %f, %f -> %f\n", l_centers[j * dims + tid], l_data, tmp[tid]);
			}
			else
				tmp[tid] = 0;
			barrier(CLK_LOCAL_MEM_FENCE);

			for (unsigned int d = 1; d < tdim; d *= 2) {
				if ((tid % (d * 2)) == 0)
					tmp[tid] += tmp[tid + d];
				barrier(CLK_LOCAL_MEM_FENCE);
			}

//			if (get_group_id(0) == 0 && tid == 0)
//				printf("%f\n", tmp[0]);
			if (tid == 0) {
				if (tmp[0] < closest_center_dis || closest_center == -1) {
					closest_center = j;
					closest_center_dis = tmp[0];
				}
				output[i] = closest_center;
			}
		}
//	}
	return ;
}
