#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string>
#include <fstream>

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "Comm.h"
#include "Context.h"
#include "QueueManager.h"
#include "BlockManager.h"
#include "TaskManager.h"

using namespace acc_runtime;

int main(int argc, char** argv) {
  
  // TODO: load conf from proto
  //int port = 1027;
  //std::string ip_address = "127.0.0.1";
  std::string conf_path = "./conf.prototxt";

  if (argc < 2) {
    printf("USAGE: %s <conf_path>\n", argv[0]);
    return -1;
  }

  int file_handle = open(argv[1], O_RDONLY);

  if (file_handle < 0) {
    printf("cannot find configure file: %s\n",
        conf_path.c_str());
    return -1;
  }
  
  ManagerConf *conf = new ManagerConf();
  google::protobuf::io::FileInputStream fin(file_handle);
  
  if (!google::protobuf::TextFormat::Parse(&fin, conf)) {
    printf("cannot parse configuration from %s\n", argv[1]);  
    return 1;
  }

  std::string ip_address = conf->ip_addr();
  int port = conf->port();

  int verbose = 3;
  if (conf->has_verbose()) {
    verbose = conf->verbose();  
  }
  // setup Logger
  Logger logger(verbose);

  // setup QueueManager
  QueueManager queue_manager(&logger);

  // setup acc context
  Context context(conf, &logger, &queue_manager);

  queue_manager.startAll();

  Comm comm(
          &context, 
          &queue_manager, 
          &logger, ip_address, port);

  comm.listen();

  return 0;
}
