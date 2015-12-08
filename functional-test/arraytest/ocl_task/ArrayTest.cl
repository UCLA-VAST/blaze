__kernel
void arrayTest(
		int weight_length,
		int num_items,
		int item_length,
		__global double* val,
		__global double* a, 
		__global double* b 
	)
{
	int gid = get_group_id(0);
	int gdim = get_num_groups(0);

	int tid = get_local_id(0);
	int tdim = get_local_size(0);

  for (int i = gid; i < num_items; i+=gdim) {
		for (int j = tid; j < item_length; j+=tdim) {
      double val_sum = 0.0;
      for (int k = 0; k < weight_length; k++) {
        val_sum += val[k];
      }
      b[i * item_length + j] = a[i * item_length +j] + val_sum;
		}
  }
}
