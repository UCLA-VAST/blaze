#include <ctype.h>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cstdint>
#include <vector>
#include <string>

#include <glog/logging.h>

#include "blaze/Client.h"

using namespace blaze;

class PiClient: public Client {
public:

  PiClient(): Client("Pi", 1, 1) {;}

  void compute() {
    int       num_pairs  = getInputNumItems(0);
    double*   data_ptr   = (double*)getInputPtr(0);
    uint32_t* output_ptr = (uint32_t*)createOutput(0, 
                              2, 1, sizeof(uint32_t));

    uint32_t inside_count = 0;
    uint32_t outside_count = 0;

    for (int i=0; i<num_pairs; i++) {
      double x = data_ptr[i*2+0];
      double y = data_ptr[i*2+1];

      if (x*x+y*y < 1.0) {
        inside_count++;
      }
      else {
        outside_count++;
      }
    }
    output_ptr[0] = inside_count;
    output_ptr[1] = outside_count;
  } 
};

int main(int argc, char** argv) {
 
  // GLOG configuration
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  FLAGS_v = 2;

  std::vector<std::pair<double, double> > all_data;
  for (std::string line; std::getline(std::cin, line); ) 
  {
    std::stringstream ss(line);

    std::string key;
    std::string value;
    double x = 0;
    double y = 0;

    std::getline(ss, key, '\t');

    try {
      std::getline(ss, value, ',');
      x = std::stof(value);
      std::getline(ss, value);
      y = std::stof(value);
    } catch (std::exception &e) {
      continue; 
    }
    all_data.push_back(std::make_pair(x, y));
  }

  // send to accelerator
  PiClient client;

  // prepare input data
  double* data_ptr = (double*)client.createInput(0, all_data.size(), 2, sizeof(double), BLAZE_INPUT);

  for (int i=0; i<all_data.size(); i++) {
    data_ptr[i*2+0] = all_data[i].first;
    data_ptr[i*2+1] = all_data[i].second;
  }

  // start computation
  client.start();

  // get output pointer
  uint32_t* output_ptr = (uint32_t*)client.getOutputPtr(0);
  
  printf("0\t%d\n", output_ptr[0]);
  printf("1\t%d\n", output_ptr[1]);

  return 0;
}
