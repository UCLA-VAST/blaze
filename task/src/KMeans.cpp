#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>

#include "blaze.h" 

using namespace blaze;

class KMeans : public Task {
public:

  // extends the base class constructor
  // to indicate how many input blocks
  // are required
  KMeans(TaskEnv* env): Task(env, 3) {;}

  // overwrites the readLine runction
  virtual char* readLine(
      std::string line, 
      size_t &num_elements, 
      size_t &num_bytes) 
  {

    std::vector<double>* v = new std::vector<double>;

    std::istringstream iss(line);

    std::copy(std::istream_iterator<double>(iss),
        std::istream_iterator<double>(),
        std::back_inserter(*v));

		size_t dims = v->size();
	  num_bytes = dims * sizeof(double);
    num_elements = dims;

    return (char*) v;
  }

  // overwrites the compute function
  virtual void compute() {

    // get input data length
    int data_length = getInputLength(0);
    int	center_length = getInputLength(1);

    // get the pointer to input/output data
    double* data     = (double*)getInput(0);
    double* centers  = (double*)getInput(1);
		int* dims = (int*)getInput(2);
		int num_points = data_length / dims[0];
		int num_centers = center_length / dims[0];

    int* closest_centers = (int*)getOutput(
                                0, 
                                1, 
																num_points,
                                sizeof(int));

    if (!data || !centers || !closest_centers) {
			fprintf(stderr, "Cannot get data pointers\n");
      throw std::runtime_error("Cannot get data pointers");
    }

		for (int i = 0; i < num_points; ++i) {
			int closest_center = -1;
			double closest_center_dis = 0;
			for (int j = 0; j < num_centers; ++j) {
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
			closest_centers[i] = closest_center;
		}
  }
};

extern "C" Task* create(TaskEnv* env) {
  return new KMeans(env);
}

extern "C" void destroy(Task* p) {
  delete p;
}
