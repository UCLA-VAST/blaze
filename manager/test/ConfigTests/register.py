#!/usr/bin/env python

import sys, socket
import struct
import task_pb2
import base64

def send_msg(sock, msg):
  msg_bytes = msg.SerializeToString()
  size = len(msg_bytes)
  size_str = struct.pack('<I', size)
  sock.send(size_str)
  sent_size = sock.send(msg_bytes)
  print ("Send %d our of %d" % (sent_size, size))

def recv_msg(sock):
  size_str = sock.recv(4);
  size = struct.unpack('<I', size_str)
  msg_bytes = sock.recv(size[0])
  msg = task_pb2.TaskMsg()
  msg.ParseFromString(msg_bytes)
  return msg;

# connect to manager
#ip = "127.0.0.1"
ip = sys.argv[1]
port = 1027

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)

try:
  sock.connect((ip, port))
except socket.error:
  print "cannot connect to ", ip, ":", port 
  sys.exit(-1)

req_msg = task_pb2.TaskMsg()
req_msg.type = task_pb2.ACCREGISTER
req_msg.acc.acc_id = "test1"
req_msg.acc.platform_id = "cpu"

# load file for test1
print "start reading file"
f = open('test1/loopback.so', 'rb')
task_impl = f.read()
req_msg.acc.task_impl = task_impl
print "start sending msg"

send_msg(sock, req_msg)

print "finish sending msg"

# load reply
reply_msg = recv_msg(sock)
if reply_msg.type != task_pb2.ACCFINISH:
  print "acc register failed: ", reply_msg.msg

