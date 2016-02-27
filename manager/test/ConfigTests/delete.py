#!/usr/bin/env python

import argparse
import sys
from blaze_client import blaze_client

# argument parser
parser = argparse.ArgumentParser(description='A Testing Client for AccRegister')
parser.add_argument('--ip', dest='ip', type=str, default='127.0.0.1',
                    help='The host IP of the destination NAM')
parser.add_argument('--port', dest='port', default=1027, help='port of the destination NAM')

# connect to manager
args = parser.parse_args()
ip = args.ip
port = args.port

client = blaze_client(ip, port)

# add acc info
client.set_acc_info("test1", "cpu")

# delete
if client.delete_acc():
  sys.exit(0)
else:
  sys.exit(-1)
