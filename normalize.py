# parameters
input_path = "positions.vec"
output_path = "positions.txt"
# end parameters


import sys
import math

input = open(input_path)
output = open(output_path,"w")
limit = -1
maxpos = -1
minpos = -1
size = 0
line = input.readline()
while line:
    size += 1
    line = line.replace("(","").replace(")","")
    vector = line.split(",")

    limit = max(int(vector[1]), limit)
    maxpos = max(int(vector[0]), maxpos)

    pos = int(vector[0])
    minpos = pos if minpos == -1 else min(pos, minpos)

    output.write(line)

    line = input.readline()


print("{0},{1},{2},{3}".format(minpos, maxpos, limit, size))

output.close()
input.close()