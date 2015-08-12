#pragma OPENCL EXTENSION cl_khr_fp64 : enable

__kernel //__attribute__ ((reqd_work_group_size(1, 1, 1)))
void run(
  __global double *data,
	int num_data,
	__global double *centers,
	int num_centers,
	int dims,
	__global int *output) {

	int i = get_global_id(0);
	int nthreads = get_global_size(0);

	__attribute__ ((xcl_pipeline_loop))
	for (; i < num_data; i += nthreads) {
		int closest_center = -1;
		double closest_center_dis = 0;

		__attribute__ ((xcl_pipeline_loop))
		for (int j = 0; j < num_centers; ++j) {
			double dis = 0;

			__attribute__ ((xcl_pipeline_loop))
			for (int d = 0; d < dims; ++d) {
				dis += 	(centers[j * dims + d] - data[i * dims + d]) * 
								(centers[j * dims + d] - data[i * dims + d]);
			}
			if (dis < closest_center_dis || closest_center == -1) {
				closest_center = j;
				closest_center_dis = dis;
			}
		}
		output[i] = closest_center;

	}
	return ;
}
