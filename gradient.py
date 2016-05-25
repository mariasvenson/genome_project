import sys
import os
import math


def printh(f, str):
    f.write(str)

def band(r,g,b):
    b = format( b, 'x' ).zfill(2)
    r = format( r, 'x' ).zfill(2)
    g = format(g, 'x').zfill(2)

    return "<tr><td width='50' height='' bgcolor='#%s%s%s'></td></tr>" % (r,g,b)


def print_gradient(f, step):
    number = 256
    max = number-1
    partitions = 6

    for i in range(0,number,step):
        printh(f, band(i,0,0))

    for i in range(0, number, step):
        printh(f, band(max, i, 0))

    for i in range(0, number, step):
        printh(f, band(max, max, i))

    for i in range(number-step,0,-step):
        printh(f, band(i, max, max))


    for i in range(number-step, 0, -step):
        printh(f, band(0, i, max))


    for i in range(number-step, 0, -step):
        printh(f, band(0, 0, i))

    return round( partitions * (max/step) - 3 )


def gradient(path, max):
    f = open(path, "w")

    printh(f, "<table cellspacing='0'' cellpadding='0' height='300'>")
    levels = print_gradient(f, 64)
    printh(f, "</table>")

    f.close()

    return levels

def main():
    gradient("gradient.html", 200)

if "__main__" == __name__:
    main()