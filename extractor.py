#!/usr/bin/python

####
# Usage: python extractor.py filename
####

import re
import os
import string
import sys
from bs4 import BeautifulSoup

MIN_RATING = -100

class Extractor (object):

    def __init__(self):
        self.commentcount = 0
    #for subdirname in os.listdir(sys.argv[1]):
        #here is where I'll make a file and append the extracted info to it
    #    f = open(subdirname, 'w') # one file per song



    #    f.write(str(count) + "\n")
    #    count += 1
    #    f.close()

    def extractLyrics(self, soup):
        lyrics = soup.find(attrs={'name' : "description"})
        return lyrics['content']

    '''
        Expects one song page. Returns all the comments appended to one string
    '''
    def extractComments(self, soup, minRating):
        ## this will get all the top-level comments ##
        comments = soup.find_all(id=re.compile("comrate-.*"))
        for comment in comments:
            rating = comment.find(id=re.compile("ratecomment-.*"))
            if int(rating.strong.string) >= minRating:
                divs = comment.parent.find("div",{"class":"com-floatright"})
                if divs is not None:
                    self.commentcount += 1
                    for item in divs.previous_siblings: #keep in mind this prints the lines in reverse order
                        if type(item).__name__ =="NavigableString": #omits <br/> tags
                            print self.commentcount,item.string.strip()

        ## this will get all replies ##
        replies = soup.find_all("div",{"class":"com-subreply-right"})
        for reply in replies:
            if int(reply.span.strong.string) >= minRating:
                self.commentcount += 1
                print self.commentcount,reply.contents[2].string.strip()

    def count(self):
        return self.commentcount


def main():
    #remove offensive items before handing it to BS
    #they used some trickery to prevent me from reading the important content
    #but they can't stop me muahahaha
    html = open(sys.argv[1]).read()
    html = html.replace(r'class="protected"','')
    html = re.sub(r'<sc.*r.*ipt>.*</sc.*r.*ipt>','',html.lower())
    soup = BeautifulSoup(html)

    #print extractLyrics(soup)
    e = Extractor()
    e.extractComments(soup, MIN_RATING)
    print "total!", e.count()

if __name__ == "__main__":
    main()
