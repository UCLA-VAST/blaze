#include <ctype.h>
#include <time.h>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <string>

int main(int argc, char** argv) {
 
  srand(time(NULL));

  for (std::string line; std::getline(std::cin, line); ) 
  {
    printf("key\t%f,%f\n", 
        (double)rand()/RAND_MAX, 
        (double)rand()/RAND_MAX);
  }

  return 0;
}
