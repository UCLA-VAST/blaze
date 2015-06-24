#!/usr/bin/python
import random

with open("testInput", "w") as f:
	for i in range(0, 1000):
		f.write(str(random.uniform(0, 1000)) + "\n")

