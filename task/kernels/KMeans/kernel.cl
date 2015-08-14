//#pragma OPENCL EXTENSION cl_khr_fp64 : enable

__kernel //__attribute__ ((reqd_work_group_size(1, 1, 1)))
void run(
  __global float *data,
	int num_data,
	__global float *centers,
	int num_centers,
	int dims,
	__global int *output) {

	int i = get_group_id(0);
	int nthreads = get_num_groups(0);

	float tmp[1024];
	float l_centers[4096];

	__attribute__ ((xcl_pipeline_loop))
	for (int j = 0; j < num_centers; ++j) {
		__attribute__ ((xcl_pipeline_loop))
		for (int d = 0; d < 784; ++d) { // FIXME
			l_centers[j * dims + d] = centers[j * dims + d];
		}
	}

	__attribute__ ((xcl_pipeline_loop))
	for (; i < num_data; i += nthreads) {
		int closest_center = -1;
		float closest_center_dis = 0;

		float l_data[784];

		__attribute__ ((xcl_pipeline_loop))
		for (int d = 0; d < 784; ++d)
			l_data[d] = data[i * dims + d];

		__attribute__ ((xcl_pipeline_loop))
		for (int j = 0; j < num_centers; ++j) {
			float dis = 0;

			__attribute__ ((xcl_pipeline_loop))
			for (int d = 0; d < 784; ++d) { // FIXME
				dis += 	(l_centers[j * dims + d] - l_data[d]) * 
								(l_centers[j * dims + d] - l_data[d]);
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
