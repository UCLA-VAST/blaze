#ifndef LOGGER_H
#define LOGGER_H

#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <cstdint>
#include <string>
#include <stdexcept>

#include <boost/lexical_cast.hpp>
#include <boost/thread/thread.hpp>

#include "Common.h"

namespace blaze {

  uint64_t getUs() {
    struct timespec tr;
    clock_gettime(CLOCK_REALTIME, &tr);

    return (uint64_t)tr.tv_sec*1e6 + tr.tv_nsec/1e3;
  }

  uint64_t getMs() {
    struct timespec tr;
    clock_gettime(CLOCK_REALTIME, &tr);

    return (uint64_t)tr.tv_sec*1e3 + tr.tv_nsec/1e6;
  }

  // get timestamp
  std::string getTS() {

    struct timespec tr;
    clock_gettime(CLOCK_REALTIME, &tr);
    struct tm *l_time = localtime(&tr.tv_sec);
    char t_str[100];
    strftime(t_str, sizeof(t_str), "%Y-%m-%d %H:%M:%S", l_time);

    std::string ts_str(t_str);
    std::string us_str = std::to_string((long long int)tr.tv_nsec/1000);

    int num_zero = 6 - us_str.size();
    for(int i = 0; i < num_zero; ++i) {
      us_str = "0" + us_str;
    }

    ts_str += "." + us_str;

    return ts_str;
  }

  // get current thread id
  std::string getTid() {
    //long long int tid = 0;
    std::string tid_str = boost::lexical_cast<std::string>(boost::this_thread::get_id());
    //sscanf(tid_str.c_str(), "%lx", &tid);

    return tid_str;
  }
}

#endif
