#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h> 
#include <string.h> 
#include <arpa/inet.h>
#include <string>
#include <fstream>

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <glog/logging.h>

#include "CommManager.h"
#include "PlatformManager.h"
#include "QueueManager.h"
#include "BlockManager.h"
#include "TaskManager.h"

using namespace blaze;


int main(int argc, char** argv) {

  google::InitGoogleLogging(argv[0]);

  FLAGS_logtostderr = 1;

  if (argc < 2) {
    printf("USAGE: %s <conf_path>\n", argv[0]);
    return -1;
  }

  std::string conf_path(argv[1]);
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

  // setup Logger
  FLAGS_v = conf->verbose();

  // setup PlatformManager
  PlatformManager platform_manager(conf);

  // check if there is accelerator successfully setup
  if (platform_manager.getLabels().empty()) {
    LOG(ERROR) << "No accelerator is setup, exiting...";
    return -1;
  }

  // check all network interfaces on this computer, and 
  // open a communicator on each interface using the same port
  int app_port = conf->app_port();
  int gam_port = conf->gam_port();

  struct ifaddrs* ifAddrStruct = NULL;
  getifaddrs(&ifAddrStruct);

  // hold all pointers to the communicator
  std::vector<boost::shared_ptr<CommManager> > comm_pool;

  for (struct ifaddrs* ifa = ifAddrStruct; 
       ifa != NULL; 
       ifa = ifa->ifa_next) 
  {
    if (!ifa->ifa_addr) {
      continue;
    }
    // check if is a valid IP4 Address
    if (ifa->ifa_addr->sa_family == AF_INET) {

      // obtain the ip address as a string
      void* tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];

      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);

      std::string ip_addr(addressBuffer);

      // create communicator for GAM
      boost::shared_ptr<CommManager> comm_gam( new GAMCommManager(
            &platform_manager, 
            ip_addr, gam_port)); 

      // create communicator for applications
      // it will start listening for new connections automatically
      boost::shared_ptr<CommManager> comm( new AppCommManager(
            &platform_manager, 
            ip_addr, app_port));

      LOG(INFO) << "Start listening " << ip_addr << " on port " <<
        app_port << " and " << gam_port;

      // push the communicator pointer to pool to avoid object
      // being destroyed out of context
      comm_pool.push_back(comm);
      comm_pool.push_back(comm_gam);
    }
  }

  while (1) {
    boost::this_thread::sleep_for(boost::chrono::seconds(60)); 
  }

  return 0;
}
