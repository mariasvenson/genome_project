import sys
import os

INPUT = "input_sample.csv"
OUTPUT  = "output.html"
SEP = ","
MAX = 500

width = 0

collection = dict()

def add(record):
    global width

    pos = int(record[1])
    score = int(record[2])
    if score in collection:
        collection[score].append(pos)
        width = max(len(collection[score]), width)
    else:
        collection[score] = [pos]

def open_html(path):
    file = open(path,"w+")
    file.write("<html></body><table width='800' height='640'>")
    return file

def close_html(file):
    file.write("</table></body></html>")
    file.close()

def collect():
    stats = open(INPUT)

    count = 0
    line = stats.readline()
    while line and count<MAX:
        add(line.split(SEP))
        line = stats.readline()
        count += 1

    stats.close()

def render_band(html, score, list):
    positions = sorted(set(list))
    html.write("<tr>")

    color = "#0000FF"
    blank = "#FFFFFF"
    print(positions)
    for i in range(1 , width):
        if i in positions:
            html.write("<td bgcolor='%s'></td>" % color)
        else:
            html.write("<td bgcolor='%s'></td>" % blank)


    html.write("</tr>")

def render():
    html = open_html(OUTPUT)

    for score in sorted(collection.keys(),reverse=True):
        render_band(html, score, collection[score])

    close_html(html)

def main():
    collect()
    render()

if "__main__" == __name__:
    main()