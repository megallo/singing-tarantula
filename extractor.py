#!/usr/bin/python

####
# Usage: python extractor.py filename
####

import re
import os
import sys
from traceback import format_exc
from bs4 import BeautifulSoup

MIN_RATING = 0
MIN_COMMENT_COUNT = 100

class Extractor (object):

    def __init__(self, soup):
        self.commentcount = 0
        self.soup = soup
        
    def totalSongComments(self):
        commentlistdiv = self.soup.find(id="comments_listing")
        comment_string = commentlistdiv.find("div").find("div").find("li").string.strip()
        comment_count = re.sub(re.compile("\D*"), "", comment_string) # removes the word 'Comments'
        print "Comment count is ! ", comment_count
        return comment_count

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
                thingy = reply.contents[2].string.strip() # a lot of these are empty lines
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
            indir, outputFileName = os.path.split(self.inputDirectory)
            
            out = open(os.path.join(self.outputDirectory,outputFileName), 'w')
            #print "outfile:",outputFileName
            needLyrics = True
            for songfile in os.listdir(self.inputDirectory):
                songfile = os.path.join(self.inputDirectory, songfile)
                #print "infile:",songfile
                #html = self.cleanHTML(songfile)
                html = open(songfile).read()
                soup = BeautifulSoup(html, "lxml")
                ext = Extractor(soup)
                if needLyrics: # only get the lyrics from the first file (it's in every file but don't want dupes)
                    #check to see if we really want to do this. I mean after all.
                    if not int(ext.totalSongComments()) >= MIN_COMMENT_COUNT:
                        print "Skipping song with too few comments"
                        out.close()
                        # it's kind of a chicken and egg situation. 
                        # easiest thing is to just delete the file we just opened
                        # (I don't like it either.)
                        os.remove(os.path.join(self.outputDirectory,outputFileName))
                        return
                    out.write(removeNonAscii(ext.extractLyrics() + '\n'))
                    needLyrics = False
		comments = removeNonAscii(ext.extractComments())
		if len(comments) < 1:
			print "Skipping song with not enough good comments"
			out.close()
			os.remove(os.path.join(self.outputDirectory,outputFileName))
			return
                out.write(removeNonAscii(ext.extractComments()))
                #print "got this many comments ", ext.count()
            # OK, that's one whole song
            out.close()
        except UnicodeEncodeError:
            print "WARNING: skipping song because of encoding issues -> %s" % outputFileName
        except IOError, e:
            print "ERROR: skipping song because of an I/O problem -> %s" % e
            print format_exc()
        except AttributeError, ee:
            print "ERROR: skipping song because I couldn't find the comment count -> %s" % self.inputDirectory
            print format_exc()
            out.close()


    def cleanHTML(self, filename):
        #remove offensive items before handing it to BS
        #they used some trickery to prevent me from reading the important content
        #but they can't stop me muahahaha
        html = open(filename).read()
        html = html.replace(r'class="protected"','')
        return re.sub(r'<sc.*r.*ipt>.*</sc.*r.*ipt>','',html.lower())

def removeNonAscii(s):
    return "".join(i for i in s if ord(i)<128)

def main():
    try:
        print "Working on directory ", sys.argv[1]
        topLevelDir = sys.argv[1]
        output = sys.argv[2]
        if not os.path.isdir(topLevelDir):
            raise Exception(topLevelDir + " is not a directory")
        
        if not os.path.exists(output):
            os.makedirs(output)
            print "Creating output directory ", output
        
        for songDir in os.listdir(topLevelDir):
            songDir = os.path.join(topLevelDir, songDir)
            if not os.path.isdir(songDir):
                print "Skipping song ", songDir, " because it doesn't look like a directory"
            else:
                try:
                    handler = SongHandler(songDir, output)
                    handler.handleSong()
                except IOError, error:
                    print "WARNING: skipping %s - maybe it's not a directory?\n%s" % (songDir, error)
                    print format_exc()
    except Exception, e:
        print "Usage: python extractor.py <dir containing song dirs> <output dir>\n %s" % e
        print format_exc()

if __name__ == "__main__":
    main()
