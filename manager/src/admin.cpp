#include <stdlib.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#define LOG_HEADER "admin"

// use flexlm
#ifdef USELICENSE
#include "license.h"
#endif

#include "blaze/Admin.h"

using namespace blaze;

#ifdef USELICENSE
void licence_check_out() {

  Feature feature = FALCON_RT;

  // initialize for licensing. call once
  fc_license_init();

  // get a feature
  fc_license_checkout(feature, 1);

  printf("\n");
}

void licence_check_in() {

  Feature feature = FALCON_RT;

  fc_license_checkin(feature);

  // cleanup for licensing. call once
  fc_license_cleanup();
}
#endif

int main(int argc, char** argv) {

  google::InitGoogleLogging(argv[0]);

  FLAGS_logtostderr = 1;
  FLAGS_v = 1;

#ifdef USELICENSE
  // check license
  licence_check_out();
#endif

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

#ifdef USELICENSE
  // release license
  licence_check_in();
#endif

  return 0;
}
