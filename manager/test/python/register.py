#!/usr/bin/env python

import argparse
import sys
import acc_conf_pb2
from google.protobuf import text_format as _text_format
from blaze_client import blaze_client

# argument parser
parser = argparse.ArgumentParser(description='A Testing Client for AccRegister')
parser.add_argument('--ip', dest='ip', type=str, default='127.0.0.1',
                    help='The host IP of the destination NAM')
parser.add_argument('--conf', dest='conf', help='path for acc configuration file')

# connect to manager
args = parser.parse_args()
ip = args.ip
conf_path = args.conf

client = blaze_client(ip)
client.register_acc(conf_path)
