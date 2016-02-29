
#include "Admin.h"

namespace blaze {

Admin::Admin(std::string _ip): 
  ip_address(_ip),
  nam_port(1027) {;}

void Admin::sendMessage(TaskMsg &msg) {

  // setup socket connection
  if (!ios || !endpoint) {
    ios_ptr _ios(new boost::asio::io_service);
    endpoint_ptr _endpoint(new boost::asio::ip::tcp::endpoint(
          boost::asio::ip::address::from_string(ip_address),
          nam_port));

    ios = _ios;
    endpoint = _endpoint;
  }

  // create socket for connection
  socket_ptr sock(new boost::asio::ip::tcp::socket(*ios));
  sock->connect(*endpoint);
  sock->set_option(boost::asio::ip::tcp::no_delay(true));

  // set socket buffer size to be 4MB
  boost::asio::socket_base::send_buffer_size option(4*1024*1024);
  sock->set_option(option); 

  send(msg, sock);

  // wait on reply for ACCREQUEST
  TaskMsg reply_msg;
  recv(reply_msg, sock);

  if (reply_msg.type() == ACCFINISH) {
    VLOG(1) << "Request successful";
  }
  else {
    LOG(ERROR) << "Request unsucessful because: " << reply_msg.msg();
  }
}

bool Admin::registerAcc(ManagerConf &conf) {


  for (int i=0; i<conf.platform_size(); i++) {
    AccPlatform platform = conf.platform(i);
    for (int j=0; j<platform.acc_size(); j++) {
      AccWorker acc = platform.acc(j); 

      TaskMsg req_msg;
      req_msg.set_type(ACCREGISTER);

      VLOG(1) << "Registering " << acc.id()
              << " on platform " << platform.id();

      AccMsg* acc_msg = req_msg.mutable_acc();
      acc_msg->set_acc_id(acc.id());
      acc_msg->set_platform_id(platform.id());
      //acc_msg->set_task_impl(readFile(acc.path()));
      acc_msg->set_task_impl(acc.path());

      for (int k=0; k<acc.param_size(); k++) {
        AccWorker::KeyValue kval = acc.param(k);
        AccMsg::KeyValue *kval_item = acc_msg->add_param();
        kval_item->set_key(kval.key());

        //if (kval.key().length() > 5 && 
        //    kval.key().substr(kval.key().length()-5, 5) == "_path")
        //{
        //  kval_item->set_value(readFile(kval.value()));
        //  DLOG(INFO) << "Setting " << kval.key() << " to contents of " 
        //             << kval.value();
        //} 
        //else 
        {
          kval_item->set_value(kval.value());
          DLOG(INFO) << "Setting " << kval.key() << " to " << kval.value();
        }
      }

      sendMessage(req_msg);
    }
  }
}

bool Admin::deleteAcc(ManagerConf &conf) {

  for (int i=0; i<conf.platform_size(); i++) {
    AccPlatform platform = conf.platform(i);
    for (int j=0; j<platform.acc_size(); j++) {
      AccWorker acc = platform.acc(j); 

      TaskMsg req_msg;
      req_msg.set_type(ACCDELETE);

      VLOG(1) << "Deleting " << acc.id()
              << " on platform " << platform.id();

      AccMsg* acc_msg = req_msg.mutable_acc();
      acc_msg->set_acc_id(acc.id());
      acc_msg->set_platform_id(platform.id());

      sendMessage(req_msg);
    }
  }
}
} // namespace blaze
