#include <boost/filesystem.hpp>
#include <fstream>
#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>
#include <stdio.h>

#include "blaze/AccAgent.h"
#include "blaze/CommManager.h"
#include "blaze/PlatformManager.h"
#include "proto/acc_conf.pb.h"

namespace blaze {

AccAgent::AccAgent(const char* conf_path): port_(1027) {
  int file_handle = open(conf_path, O_RDONLY);
  if (file_handle < 0) {
    throw fileError("AccAgent cannot find configure file");
  }
  ManagerConf_ptr conf(new ManagerConf());
  conf_ = conf;
  google::protobuf::io::FileInputStream fin(file_handle);
  if (!google::protobuf::TextFormat::Parse(&fin, conf.get())) {
    throw fileError("AccAgent cannot parse configuration file");
  }

  // Basic configurations
  FLAGS_logtostderr = 1;
  FLAGS_v           = conf_->verbose();   // logging
  int app_port      = conf_->app_port();  // port for application
  int gam_port      = conf_->gam_port();  // port for GAM

  google::InitGoogleLogging("");

  // Create local dir
  try {
    local_dir += "/nam-" + getUid();
    if (!boost::filesystem::exists(local_dir)) {
      boost::filesystem::create_directories(local_dir);
    }
    DLOG(INFO) << "Set 'local_dir' to " << local_dir;
  } catch (boost::filesystem::filesystem_error &e) {
    LOG(WARNING) << "Failed to use '" << local_dir 
                 << "' as local directory, using '/tmp' instead.";
  }

  // Create a PlatformManager
  PlatformManager_ptr platform_manager(new PlatformManager(conf_.get()));
  platform_manager_ = platform_manager;

  // Create a CommManager
  CommManager_ptr comm_manager(new AppCommManager(
      platform_manager.get(), "127.0.0.1", app_port));
  comm_manager_ = comm_manager;

  LOG(INFO) << "Finish initializing AccAgent";
}
} // namespace blaze

