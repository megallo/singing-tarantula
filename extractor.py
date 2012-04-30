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

'''
    Traverses one directory, which should correspond to
    all the HTML pages for one song.
    Writes out lyrics and all comments to Artist_Name---Song_Name
    in specified output directory
'''
class SongHandler (object):

    def __init__(self, indir, outdir):
        self.inputDirectory = indir
        self.outputDirectory = outdir

    def handleSong(self):
        for songfile in os.listdir(self.inputDirectory):
            print songfile

#        #remove offensive items before handing it to BS
#        #they used some trickery to prevent me from reading the important content
#        #but they can't stop me muahahaha
#        html = open(sys.argv[1]).read()
#        html = html.replace(r'class="protected"','')
#        html = re.sub(r'<sc.*r.*ipt>.*</sc.*r.*ipt>','',html.lower())
#        soup = BeautifulSoup(html)
#
#        #print extractLyrics(soup)
#        e = Extractor()
#        e.extractComments(soup, MIN_RATING)
#        print "total!", e.count()
    #for subdirname in os.listdir(sys.argv[1]):
        #here is where I'll make a file and append the extracted info to it
    #    f = open(subdirname, 'w') # one file per song

def main():
    # TODO: this should take the top level dir and then handle each song's dir
    handler = SongHandler(sys.argv[1],sys.argv[2])
    handler.handleSong()

if __name__ == "__main__":
    main()
