extern "C" {
void top( 
    int n, 
    double* src, 
    double* dst);
}
void top( 
    int n, 
    double* src, 
    double* dst)
{
#pragma HLS INTERFACE m_axi port=src offset=slave bundle=gmem
#pragma HLS INTERFACE m_axi port=dst offset=slave bundle=gmem
#pragma HLS INTERFACE s_axilite port=n bundle=control
#pragma HLS INTERFACE s_axilite port=src bundle=control
#pragma HLS INTERFACE s_axilite port=dst bundle=control
#pragma HLS INTERFACE s_axilite port=return bundle=control

  for (int i=0; i<n; i++) {
#pragma HLS pipeline
    dst[i] = src[i];
  }
}
