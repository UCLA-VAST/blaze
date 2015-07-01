#!/usr/bin/python
import random

with open("testInput3.txt", "w") as f:
	for i in range(0, 1966080):
		f.write(str(random.uniform(0, 1966080)) + "\n")

