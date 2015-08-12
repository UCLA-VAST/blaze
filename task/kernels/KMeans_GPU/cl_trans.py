#! /usr/bin/python
"""
Usage: <Input file>
"""
import sys

if len(sys.argv) < 2:
	print __doc__
	sys.exit(1)

name = sys.argv[1][:sys.argv[1].find('.')] + "_cl"

str = "#ifndef __KERNEL__\n"
str += "#define __KERNEL__\n"
str += "const char *" + name + " =\n"

with open(sys.argv[1], "r") as f:
	for line in f:
		str += "\""
		for ch in line[:-1]:
			if ch == '"':
				str += "\\\""
			elif ch == '\\':
				str += "\\\\"
			else:
				str += ch
		str += "\\n\"\n"
str += ";\n"
str += "#endif"

with open(name + ".h", "w") as f:
	f.write(str)
