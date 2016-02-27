import sys, socket
import struct
import task_pb2
import logging

class blaze_client:
  ip     = "127.0.0.1"
  port   = 1027
  sock   = []
  logger = []
  acc_id = []
  platform_id = []
  task_impl   = []
  buffer_size = 4*1024*1024

  def __init__(self, ip, port):
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

    buffer_size = self.buffer_size
    idx = 0
    # send with buffer_size at a time
    while idx < size - buffer_size:
      sent_size = sock.send(msg_bytes[idx:idx+buffer_size])
      if sent_size < buffer_size:
        raise OSError("Buffer does not sent correctly at idx:%d" % (idx))
      idx += buffer_size 

    # send the remaining part
    sent_size = sock.send(msg_bytes[idx:size])
    if sent_size < size-idx:
      raise OSError("Buffer does not sent correctly at idx:%d" % (idx))

  def recv_msg(self, sock):
    size_str = sock.recv(4);
    size = struct.unpack('<I', size_str)
    msg_bytes = sock.recv(size[0])
    msg = task_pb2.TaskMsg()
    msg.ParseFromString(msg_bytes)
    return msg

  def set_acc_info(self, acc_id, platform_id):
    self.acc_id = acc_id
    self.platform_id = platform_id

  def load_task(self, path):
    # load file for test1
    f = open(path, 'rb')
    self.task_impl = f.read()
    f.close()

  def register_acc(self):
    sock = self.sock
    ip = self.ip
    port = self.port
    logger = self.logger
    try:
      sock.connect((ip, port))
    except socket.error, (val, msg):
      logger.error("cannot connect to %s:%d: %s", ip, port, msg)
      sys.exit(-1)
    
    # build message to register
    req_msg = task_pb2.TaskMsg()
    req_msg.type = task_pb2.ACCREGISTER
    req_msg.acc.acc_id      = self.acc_id
    req_msg.acc.platform_id = self.platform_id
    req_msg.acc.task_impl   = self.task_impl
    
    self.send_msg(sock, req_msg)
    
    # load reply
    reply_msg = self.recv_msg(sock)
    if reply_msg.type != task_pb2.ACCFINISH:
      logger.error("acc register failed: %s", reply_msg.msg)
    else:
      logger.info("Successfully registered a new accelerator")

  def delete_acc(self):
    sock = self.sock
    ip = self.ip
    port = self.port
    logger = self.logger

    try:
      sock.connect((ip, port))
    except socket.error, (val, msg):
      logger.error("cannot connect to %s:%d: %s", ip, port, msg)
      return False

    # build a message to delete
    req_msg = task_pb2.TaskMsg()
    req_msg.type = task_pb2.ACCDELETE
    req_msg.acc.acc_id = self.acc_id 
    req_msg.acc.platform_id = self.platform_id
    
    self.send_msg(sock, req_msg)
    
    # load reply
    reply_msg = self.recv_msg(sock)
    if reply_msg.type != task_pb2.ACCFINISH:
      logger.error("Acc deletion failed: %s", reply_msg.msg)
      return False
    else:
      logger.info("Successfully removed an accelerator")
      return True

