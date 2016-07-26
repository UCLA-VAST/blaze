#pragma OPENCL EXTENSION cl_khr_fp64 : enable

__kernel
void reduction(__local double* partial_sums) {
  int tid = get_local_id(0);
  int gdim = get_local_size(0);
  barrier(CLK_LOCAL_MEM_FENCE);
  for(int i = gdim/2; i>0; i >>= 1) {
    if(tid < i) {
      partial_sums[tid] += partial_sums[tid + i];
    }
    barrier(CLK_LOCAL_MEM_FENCE);
  }
}

__kernel
void gradient(
		int num_samples,
		int label_size,
		int feature_size,
		__global double* weights,
		__global double* data, 
		__global double* output)
{
	int gid = get_group_id(0);
	int gdim = get_num_groups(0);

	int tid = get_local_id(0);
	int tdim = get_local_size(0);

  const int L = label_size;
  const int D = feature_size; 
  const int weight_size = (L-1)*D;

  // maximum label size = 32
  double margins[32];
  __local double l_margins[64];

  for(int j = tid; j < weight_size+1; j+=tdim) {
    output[gid*(weight_size+1)+j] = 0.0;
  }

  for(int k = gid; k < num_samples; k+=gdim) {

    double marginY = 0.0;
    double maxMargin = -INFINITY;
    int    maxMarginIndex = 0;

    double  label = data[k*(D+1)];
    __global double* feature = data + k*(D+1) + 1;

    for (int i=0; i<L-1; i++) {

      // dot product
      //double margin = 0.0;
      l_margins[tid] = 0.0;
      for(int j = tid; j < D; j+=tdim) {
        l_margins[tid] += weights[i*D+j] * feature[j];
      }
      // reduction
      reduction(l_margins);

      double margin = l_margins[0];
      if (i == (int)label - 1) {
        marginY = margin;
      }
      if (margin > maxMargin) {
        maxMargin = margin;
        maxMarginIndex = i;
      }
      margins[i] = margin;
    }

    double sum = 0.0;
    for (int i=0; i<L-1; i++) {
      if (maxMargin > 0) {
        margins[i] -= maxMargin;
        if (i == maxMarginIndex) {
          sum += exp(-maxMargin);
        }
        else {
          sum += exp(margins[i]);
        }
      } 
      else {
        sum += exp(margins[i]);
      }
    }

    // update gradient
    for(int i = 0; i < L-1; i++ ) {
      double multiplier = exp(margins[i]) / (sum+1.0);
      if (label != 0.0 && label == i+1) {
        multiplier -= 1.0;
      }
      for (int j = tid; j < D; j += tdim) {
        output[gid*(weight_size+1)+i*D+j] += multiplier*feature[j];
      }
    }

    // compute loss
    double loss = log(sum+1); // math.logip(sum)
    if (label > 0.0) {
      loss -= marginY;
    }
    if (maxMargin > 0) {
      loss += maxMargin;
    }
    if (tid == 0) {
      output[gid*(weight_size+1)+weight_size] += loss;
    }
  }
}

__kernel
void vector_sum(
		int vector_size,
    int num_vectors,
    __global double* in,
    __global double* out)
{
	int tid = get_local_id(0);
	int tdim = get_local_size(0);

  double results[32];

  for (int k=0; k<vector_size; k+=32*tdim) {
    for (int i=0; i<num_vectors; i++) {
      for (int j=tid; j<vector_size-k && j<32*tdim; j+=tdim) {
        if (i==0) {
          results[j/tdim]  = in[i*vector_size+k+j];
        } else {
          results[j/tdim] += in[i*vector_size+k+j];
        }
      }
    }
    for (int j=tid; j<vector_size-k && j<32*tdim; j+=tdim) {
      out[k+j] = results[j/tdim];
    }
  }
}
