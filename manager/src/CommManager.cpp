#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <fcntl.h>   
#include <fstream>
#include <glog/logging.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>

#include "blaze/CommManager.h"
#include "blaze/PlatformManager.h"
#include "blaze/BlockManager.h"
#include "blaze/TaskManager.h"

#define MAX_MSGSIZE 4096

namespace blaze {

CommManager::CommManager(
      PlatformManager* _platform,
      std::string _address, int _port,
      int _max_threads):
    ip_address(_address), 
    srv_port(_port), 
    platform_manager(_platform)
{ 
  // create io_service pointers
  ios_ptr      _ios(new io_service);
  endpoint_ptr _endpoint(new ip::tcp::endpoint(
                  ip::address::from_string(ip_address), 
                  srv_port));
  acceptor_ptr _acceptor(new ip::tcp::acceptor(*_ios, *_endpoint));

  ios = _ios;
  endpoint = _endpoint;
  acceptor = _acceptor;

  // start io service processing loop
  io_service::work work(*ios);

  for (int t=0; t<_max_threads; t++) 
  {
    comm_threads.create_thread(
        boost::bind(&io_service::run, ios.get()));
  }

  // asynchronously start listening for new connections
  startAccept();

  VLOG(2) << "Listening for new connections at "
    << ip_address << ":" << srv_port;
}

CommManager::~CommManager() {
  DLOG(INFO) << "Destroyer called";
  ios->stop();
}

void CommManager::startAccept() {
  socket_ptr sock(new ip::tcp::socket(*ios));
  acceptor->async_accept(*sock,
      boost::bind(&CommManager::handleAccept,
                  this,
                  boost::asio::placeholders::error,
                  sock));
}

void CommManager::handleAccept(
    const boost::system::error_code& error,
    socket_ptr sock) 
{
  if (!error) {
    ios->post(boost::bind(&CommManager::process, this, sock));
    startAccept();
  }
}
} // namespace blaze
