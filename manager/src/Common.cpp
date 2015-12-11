#ifndef LOGGER_H
#define LOGGER_H

#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <cstdint>
#include <string>
#include <stdexcept>

#include <syscall.h>
#include <sys/syscall.h>
#include <unistd.h>

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
  // using the same code from googlelog/src/utilities.cc
  // without OS checking
  uint32_t getTid() {
    static bool lacks_gettid = false;

    if (!lacks_gettid) {
      pid_t tid = syscall(__NR_gettid);
      if (tid != -1) {
        return (uint32_t)tid;
      }
      // Technically, this variable has to be volatile, but there is a small
      // performance penalty in accessing volatile variables and there should
      // not be any serious adverse effect if a thread does not immediately see
      // the value change to "true".
      lacks_gettid = true;
    }

    // If gettid() could not be used, we use one of the following.
    return (uint32_t)getpid(); 
  }

  void log(FILE* out, const std::string msg) {

    fprintf(out, "[%s:%u] %s\n", 
        getTS().c_str(), getTid(), msg.c_str());

    fflush(out);
  }

  void logInfo(const std::string msg) {
    log(stdout, msg);
  }

  void logErr(const std::string msg) {
    log(stderr, msg);
  }
}

#endif
