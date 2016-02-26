__kernel
void arrayTest(
		int num_vectors,
		int vector_length,
		__global double* a, 
		__global double* b,
		__global double* c
	)
{
	int gid = get_group_id(0);
	int gdim = get_num_groups(0);

	int tid = get_local_id(0);
	int tdim = get_local_size(0);

  for (int i = gid; i < num_vectors; i+=gdim) {
		for (int j = tid; j < vector_length; j+=tdim) {
      c[i * vector_length + j] = a[i * vector_length + j] + b[j];
		}
  }
}
