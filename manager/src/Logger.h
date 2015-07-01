#include <stdio.h>
#include <sys/time.h>
#include <time.h>

#include <cstdint>
#include <string>
#include <stdexcept>
#include <boost/lexical_cast.hpp>

class Logger {

public:
  Logger(int v=3): verbose(v) {
    // default output direct
    info_out = stdout;
    error_out = stderr;
    file_opened = false;
  }
  Logger(std::string info_file, std::string error_file, int v): 
      verbose(v) 
  {
    info_out = fopen(info_file.c_str(), "w+");
    error_out = fopen(error_file.c_str(), "w+");
    if (!info_out || !error_out) {
      throw std::runtime_error("Cannot find output files");
    }
    file_opened = true;
  }

  ~Logger() {
    fclose(info_out);
    fclose(error_out);
  }

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

  void logInfo(const std::string msg) {
    log(info_out, msg);
  }

  void logErr(const std::string msg) {
    log(error_out, msg);
  }

private:

  void log(FILE* out, const std::string msg) {
    if (verbose >= 3) {
      fprintf(out, "[%s:%s] %s\n", 
          this->getTS().c_str(), 
          this->getTid().c_str(), 
          msg.c_str());
    }
    else if (verbose == 2) {
      fprintf(out, "[%s] %s\n", 
          this->getTS().c_str(), 
          msg.c_str());
    }
    else if (verbose == 1) {
      fprintf(out, "%s\n", msg.c_str());
    }   
    fflush(out);
  }
  int verbose;
  bool file_opened;
  FILE* info_out;
  FILE* error_out;
};
