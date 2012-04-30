#!/usr/bin/python

####
# Usage: python extractor.py filename
####

import re
import os
import sys
from traceback import format_exc
from bs4 import BeautifulSoup

MIN_RATING = -100

class Extractor (object):

    def __init__(self, soup):
        self.commentcount = 0
        self.soup = soup

    def extractLyrics(self):
        lyrics = self.soup.find(attrs={'name' : "description"})
        return lyrics['content']

    '''
        Expects one song page. Returns all the comments appended to one string
    '''
    def extractComments(self):
        returnMe = []
        ## this will get all the top-level comments ##
        comments = self.soup.find_all(id=re.compile("comrate-.*"))
        for comment in comments:
            rating = comment.find(id=re.compile("ratecomment-.*"))
            if int(rating.strong.string) >= MIN_RATING:
                divs = comment.parent.find("div",{"class":"com-floatright"})
                if divs is not None:
                    self.commentcount += 1
                    for item in divs.previous_siblings: #keep in mind this prints the lines in reverse order
                        if type(item).__name__ =="NavigableString": #omits <br/> tags
                            thingy = item.string.strip() # a lot of these are empty lines :(
                            if len(thingy) != 0:
                                returnMe.append(thingy)
                                returnMe.append('\n')

        ## this will get all replies ##
        replies = self.soup.find_all("div",{"class":"com-subreply-right"})
        for reply in replies:
            if int(reply.span.strong.string) >= MIN_RATING:
                self.commentcount += 1
                thingy = reply.contents[2].string.strip() # a lot of these are empty lines :(
                if len(thingy) != 0:
                    returnMe.append(thingy)
                    returnMe.append('\n')


        return ''.join(returnMe)

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
        if not (os.path.isdir(indir) and os.path.isdir(outdir)):
            raise IOError("Give me directory names")
        self.inputDirectory = indir
        self.outputDirectory = outdir

    def handleSong(self):
        try:
            # use the artist and song name from the input file
            outputFileName = os.listdir(self.inputDirectory)[0]
            outputFileName = re.split(re.compile("__\d+$"),outputFileName)[0]
            out = open(os.path.join(self.outputDirectory,outputFileName), 'w')
            print "outfile:",outputFileName
            needLyrics = True
            for songfile in os.listdir(self.inputDirectory):
                songfile = os.path.join(self.inputDirectory, songfile)
                print "infile:",songfile
                html = self.cleanHTML(songfile)
                soup = BeautifulSoup(html)
                ext = Extractor(soup)
                if needLyrics:
                    out.write(ext.extractLyrics() + '\n')
                    needLyrics = False
                out.write(ext.extractComments())
                print "got this many comments ", ext.count()
            # OK, that's one whole song
            out.close()
        except IndexError:
            print "WARNING: skipping song because the directory is empty -> %s" % self.inputDirectory


    def cleanHTML(self, filename):
        #remove offensive items before handing it to BS
        #they used some trickery to prevent me from reading the important content
        #but they can't stop me muahahaha
        html = open(filename).read()
        html = html.replace(r'class="protected"','')
        return re.sub(r'<sc.*r.*ipt>.*</sc.*r.*ipt>','',html.lower())

#
#        #print extractLyrics(soup)
#        e = Extractor()
#        e.extractComments(soup, MIN_RATING)
#        print "total!", e.count()
    #for subdirname in os.listdir(sys.argv[1]):
        #here is where I'll make a file and append the extracted info to it
    #    f = open(subdirname, 'w') # one file per song

def main():
    try:
        topLevelDir = sys.argv[1]
        output = sys.argv[2]
        for songDirs in os.listdir(topLevelDir):
            try:
                handler = SongHandler(songDirs, output)
                handler.handleSong()
            except IOError, error:
                print "WARNING: skipping %s - maybe it's not a directory?\n%s" % (songDirs, error)
                print format_exc()
    except Exception, e:
        print "Usage: python extractor.py <dir containing song dirs> <output dir>\n %s" % e
        print format_exc()

if __name__ == "__main__":
    main()
