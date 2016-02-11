#!/usr/bin/env python

import sys, socket
import struct
import task_pb2

def send_msg(sock, msg):
  msg_bytes = msg.SerializeToString()
  size = len(msg_bytes)
  size_str = struct.pack('<I', size)
  sock.send(size_str)
  sock.send(msg_bytes)

def recv_msg(sock):
  size_str = sock.recv(4);
  size = struct.unpack('<I', size_str)
  msg_bytes = sock.recv(size[0])
  msg = task_pb2.TaskMsg().ParseFromString(msg_bytes)
  return msg;

# connect to manager
ip = "127.0.0.1"
port = 1027

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
  sock.connect((ip, port))
except socket.error:
  print "cannot connect to ", ip, ":", port 
  sys.exit(-1)

req_msg = task_pb2.TaskMsg()
req_msg.type = task_pb2.ACCREQUEST
req_msg.app_id = "test-python"
req_msg.acc_id = "ArrayTest"
acc_data = req_msg.data.add()
acc_data.partition_id = 0

send_msg(sock, req_msg)
acc_msg = recv_msg(sock)

