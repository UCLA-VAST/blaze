import sys, socket
import struct
import acc_conf_pb2
import task_pb2
import logging
import array
from google.protobuf import text_format as _text_format

class blaze_client:
  ip     = "127.0.0.1"
  port   = 1027
  sock   = []
  logger = []
  buffer_size = 4*1024*1024

  def __init__(self, ip):
    
    self.ip = ip

    # setup socket
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.buffer_size)

    # logging
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)
    self.logger = logging.getLogger()
    self.logger.setLevel(logging.INFO)

  def send_msg(self, sock, msg):
    # serialize msg to bytes
    msg_bytes = msg.SerializeToString()

    # send the total size
    size = len(msg_bytes)
    size_str = struct.pack('<I', size)
    sock.send(size_str)

    buffer_size = 8192

    idx = 0
    # send with buffer_size at a time
    while idx < size - buffer_size:
      sent_size = sock.send(msg_bytes[idx:idx+buffer_size])
      print "sent", sent_size, " out of", buffer_size
      if sent_size < buffer_size:
        raise OSError("Buffer does not sent correctly at idx:%d" % (idx))
      idx += buffer_size 

    # send the remaining part
    sent_size = sock.send(msg_bytes[idx:size])
    print "sent", sent_size, " out of", size-idx
    if sent_size < size-idx:
      raise OSError("Buffer does not sent correctly at idx:%d" % (idx))

  def recv_msg(self, sock):
    size_str = sock.recv(4);
    size = struct.unpack('<I', size_str)
    msg_bytes = sock.recv(size[0])
    msg = task_pb2.TaskMsg()
    msg.ParseFromString(msg_bytes)
    return msg

  def read_binary_file(self, path):
    # load file for test1
    f = open(path, 'rb')
    ret = f.read()
    f.close()
    return ret;

  def register_acc(self, conf_path):
    sock = self.sock
    ip = self.ip
  
    # load and parse conf from file
    f = open(conf_path, 'r')
    config = acc_conf_pb2.ManagerConf()
    _text_format.Merge(f.read(), config)
    f.close()

    logger = self.logger
    port = config.app_port

    try:
      sock.connect((ip, port))
    except socket.error, (val, msg):
      logger.error("cannot connect to %s:%d: %s", ip, port, msg)
      sys.exit(-1)

    # load acc info from configuration
    for platform_conf in config.platform:
      for acc_conf in platform_conf.acc:

        # build message to register
        req_msg = task_pb2.TaskMsg()
    
        req_msg.type            = task_pb2.ACCREGISTER
        req_msg.acc.acc_id      = acc_conf.id
        req_msg.acc.platform_id = platform_conf.id
        req_msg.acc.task_impl   = self.read_binary_file(acc_conf.path)

        # add parameters
        for kval in acc_conf.param: 
          kval_item = req_msg.acc.param.add()  
          kval_item.key = kval.key
          if kval.key[len(kval.key)-5:] == "_path":
            kval_item.value = self.read_binary_file(kval.value)
            logger.info("Setting %s to file contents", kval.key)
          else:
            kval_item.value = str(kval.value)
            logger.info("Setting %s to %s", kval.key, str(kval.value))
        
        self.send_msg(sock, req_msg)
        
        # load reply
        reply_msg = self.recv_msg(sock)
        if reply_msg.type != task_pb2.ACCFINISH:
          logger.error("acc register failed: %s", reply_msg.msg)
        else:
          logger.info("Successfully registered a new accelerator")

  def delete_acc(self, conf_path):
    sock = self.sock
    ip = self.ip
    logger = self.logger

    # load and parse conf from file
    f = open(conf_path, 'r')
    config = acc_conf_pb2.ManagerConf()
    _text_format.Merge(f.read(), config)
    f.close()

    port = config.app_port

    try:
      sock.connect((ip, port))
    except socket.error, (val, msg):
      logger.error("cannot connect to %s:%d: %s", ip, port, msg)
      return False

    for platform_conf in config.platform:
      platform_id = platform_conf.id;
      for acc_conf in platform_conf.acc:
        acc_id   = acc_conf.id

        # build a message to delete
        req_msg = task_pb2.TaskMsg()
        req_msg.type = task_pb2.ACCDELETE
        req_msg.acc.acc_id = acc_id 
        req_msg.acc.platform_id = platform_id
    
        self.send_msg(sock, req_msg)
    
        # load reply
        reply_msg = self.recv_msg(sock)
        if reply_msg.type != task_pb2.ACCFINISH:
          logger.error("Acc deletion failed: %s", reply_msg.msg)
        else:
          logger.info("Successfully removed an accelerator")

