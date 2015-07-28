#! /usr/bin/python

import sys
import random

if len(sys.argv) < 4:
	print "generate.py <file name> <dimension> <number>"
	sys.exit(1)

dim = int(sys.argv[2])
num = int(sys.argv[3])

with open(sys.argv[1], "w") as f:
	for i in range(0, num):
		for j in range(0, dim):
			f.write(str(random.uniform(0, 9999)) + " ")
		f.write("\n")

