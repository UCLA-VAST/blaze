#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <stdlib.h>

#include "blaze/Admin.h"

using namespace blaze;

int main(int argc, char** argv) {

  google::InitGoogleLogging(argv[0]);

  FLAGS_logtostderr = 1;
  FLAGS_v = 1;

  srand(time(NULL));

  if (argc < 3) {
    printf("USAGE: %s <r|d> <conf_path> [NAM_IP]\n", argv[0]);
    return -1;
  }

  std::string conf_path(argv[2]);
  int file_handle = open(conf_path.c_str(), O_RDONLY);

  if (file_handle < 0) {
    printf("cannot find configure file: %s\n",
        argv[1]);
    return -1;
  }
  
  ManagerConf *conf = new ManagerConf();
  google::protobuf::io::FileInputStream fin(file_handle);
  

  if (!google::protobuf::TextFormat::Parse(&fin, conf)) {
    LOG(FATAL) << "cannot parse configuration from " << argv[1];
  }

  Admin admin;

  if (strcmp(argv[1], "d")) {
    admin.registerAcc(*conf);
  }
  else {
    admin.deleteAcc(*conf);
  }

  return 0;
}
