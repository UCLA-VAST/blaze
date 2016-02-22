#!/usr/bin/env python
import argparse
import sys, socket
import struct
import task_pb2
import logging

# argument parser
parser = argparse.ArgumentParser(description='A Testing Client for AccRegister')
parser.add_argument('--ip', dest='ip', type=str, default='127.0.0.1',
                    help='The host IP of the destination NAM')
parser.add_argument('--port', dest='port', default=1027, help='port of the destination NAM')

# logging
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def send_msg(sock, msg):
  # serialize msg to bytes
  msg_bytes = msg.SerializeToString()

  # send the total size
  size = len(msg_bytes)
  size_str = struct.pack('<I', size)
  sock.send(size_str)

  buffer_size = 4096
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

def recv_msg(sock):
  size_str = sock.recv(4);
  size = struct.unpack('<I', size_str)
  msg_bytes = sock.recv(size[0])
  msg = task_pb2.TaskMsg()
  msg.ParseFromString(msg_bytes)
  return msg;

# connect to manager
args = parser.parse_args()
ip = args.ip
port = args.port

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)

try:
  sock.connect((ip, port))
except socket.error, (val, msg):
  logger.error("cannot connect to %s:%d: %s", ip, port, msg)
  sys.exit(-1)

# build a message to delete
req_msg = task_pb2.TaskMsg()
req_msg.type = task_pb2.ACCDELETE
req_msg.acc.acc_id = "test1"
req_msg.acc.platform_id = "cpu"

send_msg(sock, req_msg)

# load reply
reply_msg = recv_msg(sock)
if reply_msg.type != task_pb2.ACCFINISH:
  logger.error("Acc deletion failed: %s", reply_msg.msg)
else:
  logger.info("Successfully registering an accelerator")


