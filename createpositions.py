# parameters
size = 10000
path = "positions.txt"
step = 1000
minscore = 60
maxscore = 500
# parameters end


# script
import random

pos = open(path, "w")
for i in range(1,size + 1):
    pos.write("{0},{1}\n".format(i*step, random.randint(minscore, maxscore) ))

pos.close()

# script end