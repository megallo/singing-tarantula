#!/usr/bin/python

import os
import sys
from bs4 import BeautifulSoup

count = 1
#for subdirname in os.listdir(sys.argv[1]):
    #here is where I'll make a file and append the extracted info to it
#    f = open(subdirname, 'w') # one file per song



#    f.write(str(count) + "\n")
#    count += 1
#    f.close()

def extractLyrics(filename):
    soup = BeautifulSoup(open(filename))
    lyrics = soup.find(attrs={'name' : "description"})
    return lyrics['content']

def extractComments(filename, minRating):
    f = open(filename)
    unicode(f)
    soup = BeautifulSoup(f)
    f = open("sopretty", 'w')
    f.write(soup.prettify())
    f.close()
    print soup.find(id="songText2")
    commentBlock = soup.find("a",attrs={'name' : "comment"})#find_all("div")#, id="comments_listing")
    print commentBlock
    print commentBlock.div['class']


def main():
    extractComments(sys.argv[1], 2)

if __name__ == "__main__":
    main()
