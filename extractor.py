#!/usr/bin/python

####
# Usage: python extractor.py filename
####

import re
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

def extractLyrics(soup):
    lyrics = soup.find(attrs={'name' : "description"})
    return lyrics['content']

def extractComments(soup, minRating):
#    f = open("sopretty", 'w')
#    f.write(soup.prettify())
#    f.close()
    print soup.find(id="songText2")
#    commentBlock = soup.find("a",attrs={'name' : "comment"})#find_all("div")#, id="comments_listing")
#    print commentBlock
#    print commentBlock.div['class']


def main():
    #remove offensive items before handing it to BS
    #they used some trickery to prevent me from reading the important content
    #but they can't stop me muahahaha
    html = open(sys.argv[1]).read()
    html = html.replace(r'class="protected"','')
    html = re.sub(r'<scr.*ipt>.*</scr.*ipt>','',html)
    soup = BeautifulSoup(html)

    print extractLyrics(soup)
    extractComments(soup, 2)

if __name__ == "__main__":
    main()
