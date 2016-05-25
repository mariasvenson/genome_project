# parameters
input_path = "positions.txt"
output_path = "heatmap.html"
template_path = "template.html"
scale_path = "scale.html"
size = 300
maxscore = 1500
gradient_step = 64
# end parameters

import sys
import math

html_block = {}

def gradient_color(r,g,b):
    b = format( b, 'x' ).zfill(2)
    r = format( r, 'x' ).zfill(2)
    g = format(g, 'x').zfill(2)

    return "#%s%s%s" % (r,g,b)

def create_gradient(step):
    number = 256
    max = number-1
    partitions = 6

    gradient = {}

    cursor = 0
    for i in range(0,number,step):
        gradient[i] = gradient_color(i,0,0)
        cursor += step

    start = cursor
    for i in range(0, number, step):
        gradient[i + start] = gradient_color(max, i, 0)
        cursor += step

    start= cursor
    for i in range(0, number, step):
        gradient[i + start] = gradient_color(max, max, i)
        cursor += step

    start = cursor
    for i in range(number-step,0,-step):
        gradient[i + start] = gradient_color(i,max, max)
        cursor += step

    start = cursor
    for i in range(number-step, 0, -step):
        gradient[i + start] = gradient_color(0,i, max)
        cursor += step

    start = cursor
    for i in range(number-step, 0, -step):
        gradient[i + start] = gradient_color(0,0,i)
        cursor += step

    return gradient


def get_color(number, step, gradient):
    scale = len(gradient)/maxscore
    index = math.floor(number * scale)
    return gradient.get(index * step, "#ffffff")


def create_block(gradient, gradient_step,  index, block):
    f = open("block/block_{0}.fragment.html".format(index), "w")

    widthtag =  {False : "",True: " width='2' "}[index == 0]

    for i in range(len(block)):
        f.write("<td bgcolor='{0}'{1}></td>".format(get_color(block[i][1], gradient_step, gradient), widthtag))

    f.close()

    call = "$('#block_{0}').load('block/block_{0}.fragment.html');"
    placeholder = "<tr id='block_{0}'></tr>"
    html_block["block_{0}".format(index)] = {"call" : call.format(index), "placeholder" : placeholder.format(index)}

if len(sys.argv)>2:
    input_path = sys.argv[1]

if len(sys.argv)>3:
    output_path = sys.argv[1]


# Constructing the blocks
pos = open(input_path)
gradient = create_gradient(gradient_step)
line = pos.readline()
block = []
count = 0
index = 0
while line:
    line = line.strip("\n").split(",")

    block.append([line[0], int(line[1])])
    if count == size:
        create_block(gradient, gradient_step, index, block.copy())
        block.clear()
        index += 1
        count = 0

    line = pos.readline()
    count += 1

pos.close()

# Integrating the blocks
template = open(template_path)

html = "".join( template.readlines() )

calls = ""
placeholders = ""
for key in html_block.keys():
    calls += html_block[key]["call"] + "\n"
    placeholders += html_block[key]["placeholder"] + "\n"

template.close()

html = html.replace("[[CALLS]]", calls).replace("[[PLACEHOLDERS]]", placeholders)

output = open(output_path,"w")
output.write(html)

# Decoration
scale = open(scale_path,"w")
html = "<font size='1'><table cellspacing='0' cellpadding='0' height='300'></font>"

for i in sorted(gradient.keys()):
    html += "<tr><td>{0}</td></tr>".format(i)

html += "</table>"

scale.write(html)
scale.close()
