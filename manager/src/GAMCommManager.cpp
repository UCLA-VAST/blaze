#include <stdio.h>
#include <time.h>
#include <fcntl.h>   
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <iostream>
#include <string>
#include <algorithm>
#include <vector>
#include <stdexcept>
#include <cstdint>

#include "proto/msgGamNam.pb.h"

#include "CommManager.h"

#define LOG_HEADER  std::string("GAMCommManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")
namespace blaze {

void GAMCommManager::process(socket_ptr sock) {

  // turn off Nagle Algorithm to improve latency
  sock->set_option(ip::tcp::no_delay(true));

  // set socket buffer size to be 4MB
  socket_base::receive_buffer_size option(4*1024*1024);
  sock->set_option(option); 

  Gam2NamRequest msg;
  try {
    recv(msg, sock);
    
    if (msg.type() == Gam2NamRequest::ACCNAMES) {
      
      logger->logErr(LOG_HEADER+
          std::string("Received request for ACCNAMES"));

      Nam2GamAccNames reply_msg;

      // compile a list from platform manager
      std::vector<std::string> names = platform_manager->getAccNames();

      // sort list to avoid permutations
      std::sort(names.begin(), names.end());

      if ((!msg.has_pull() || !msg.pull()) && 
          names == last_names) 
      {
        reply_msg.set_isupdated(false); 
      }
      else {
        reply_msg.set_isupdated(true); 
      
        for (int i=0; i<names.size(); i++) {
          reply_msg.add_acc_names(names[i]);
        }
        last_names = names;
      }

      // send reply message
      send(reply_msg, sock);
    }
    else {
      throw std::runtime_error("Unexpected message");
    }
  }
  catch (std::exception &e) {
    logger->logErr(LOG_HEADER+
        std::string("Failed to communicate with GAM: ")+
        std::string(e.what()));
  }
}
} // namespace blaze

