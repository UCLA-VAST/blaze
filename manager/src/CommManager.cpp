#include <stdio.h>
#include <time.h>
#include <fcntl.h>   
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <fstream>

#define LOG_HEADER "CommManager"
#include <glog/logging.h>

#include "CommManager.h"
#include "PlatformManager.h"
#include "BlockManager.h"
#include "TaskManager.h"

#define MAX_MSGSIZE 4096

namespace blaze {

void CommManager::listen() {

  try {
    io_service ios;

    ip::tcp::endpoint endpoint(
        ip::address::from_string(ip_address),
        srv_port);

    ip::tcp::acceptor acceptor(ios, endpoint);

    // start io service processing loop
    io_service::work work(ios);

    // create a thread pool
    boost::thread_group threadPool;

    for (int t=0; t<max_threads; t++) 
    {
      threadPool.create_thread(
          boost::bind(&io_service::run, &ios));
    }

    VLOG(2) << "Listening for new connections at "
      << ip_address << ":" << srv_port;

    while(1) {

      // create socket for connection
      socket_ptr sock(new ip::tcp::socket(ios));

      // accept incoming connection
      acceptor.accept(*sock);

      //boost::thread t(boost::bind(&CommManager::process, this, sock));
      // post a job to a worker thread
      ios.post(boost::bind(&CommManager::process, this, sock));
    }
    ios.stop();
  }
  catch (std::exception &e) {
    // do not throw exception, just end current thread
    LOG(ERROR) << e.what();
  }
}
} // namespace blaze
