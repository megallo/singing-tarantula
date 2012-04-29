#!/usr/bin/env python

"""Web Crawler/Spider

This module implements a web crawler. This is very _basic_ only
and needs to be extended to do anything usefull with the
traversed pages.

From: http://code.activestate.com/recipes/576551-simple-web-crawler/

"""

import re
import sys
import time
import math
import urllib2
import urlparse
import optparse
import hashlib
from cgi import escape
from traceback import format_exc
from Queue import Queue, Empty as QueueEmpty

from bs4 import BeautifulSoup

__version__ = "0.2"
__copyright__ = "CopyRight (C) 2008-2011 by James Mills"
__license__ = "MIT"
__author__ = "James Mills"
__author_email__ = "James Mills, James dot Mills st dotred dot com dot au"

####config stuff
MIN_COMMENTS = 2
###

USAGE = "%prog [options] <url>"
VERSION = "%prog v" + __version__

AGENT = "%s" % ("Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.1.4) Gecko/20091030 Gentoo Firefox/3.5.4")#, __version__)
#AGENT = "Mozilla/5.0 (Linux; U; Android 1.0; en-us; dream) AppleWebKit/525.10+ (KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2"
class Link (object):

    def __init__(self, src, dst, link_type):
        self.src = src
        self.dst = dst
        self.link_type = link_type

    def __hash__(self):
        return hash((self.src, self.dst, self.link_type))

    def __eq__(self, other):
        return (self.src == other.src and
                self.dst == other.dst and
                self.link_type == other.link_type)

    def __str__(self):
        return self.src + " -> " + self.dst

class Crawler(object):

    def __init__(self, root, depth_limit, confine=None, exclude=[], locked=True, filter_seen=True):
        self.root = root
        self.host = urlparse.urlparse(root)[1]

        ## Data for filters:
        self.depth_limit = depth_limit # Max depth (number of hops from root)
        self.locked = locked           # Limit search to a single host?
        self.confine_prefix=confine    # Limit search to this prefix
        self.exclude_prefixes=exclude; # URL prefixes NOT to visit


        self.urls_seen = set()          # Used to avoid putting duplicates in queue
        self.urls_remembered = set()    # For reporting to user
        self.visited_links= set()       # Used to avoid re-processing a page
        self.links_remembered = set()   # For reporting to user

        self.num_links = 0              # Links found (and not excluded by filters)
        self.num_followed = 0           # Links followed.

        # Pre-visit filters:  Only visit a URL if it passes these tests
        self.pre_visit_filters=[self._prefix_ok,
                                self._exclude_ok,
                                self._not_visited,
                                self._same_host]

        # Out-url filters: When examining a visited page, only process
        # links where the target matches these filters.
        if filter_seen:
            self.out_url_filters=[self._prefix_ok,
                                     self._same_host]
        else:
            self.out_url_filters=[]

    def _pre_visit_url_condense(self, url):

        """ Reduce (condense) URLs into some canonical form before
        visiting.  All occurrences of equivalent URLs are treated as
        identical.

        All this does is strip the \"fragment\" component from URLs,
        so that http://foo.com/blah.html\#baz becomes
        http://foo.com/blah.html """

        base, frag = urlparse.urldefrag(url)
        return base

    ## URL Filtering functions.  These all use information from the
    ## state of the Crawler to evaluate whether a given URL should be
    ## used in some context.  Return value of True indicates that the
    ## URL should be used.

    def _prefix_ok(self, url):
        """Pass if the URL has the correct prefix, or none is specified"""
        return (self.confine_prefix is None  or
                url.startswith(self.confine_prefix))

    def _exclude_ok(self, url):
        """Pass if the URL does not match any exclude patterns"""
        prefixes_ok = [ not url.startswith(p) for p in self.exclude_prefixes]
        return all(prefixes_ok)

    def _not_visited(self, url):
        """Pass if the URL has not already been visited"""
        return (url not in self.visited_links)

    def _same_host(self, url):
        """Pass if the URL is on the same host as the root URL"""
        try:
            host = urlparse.urlparse(url)[1]
            return re.match(".*%s" % self.host, host)
        except Exception, e:
            print >> sys.stderr, "ERROR: Can't process url '%s' (%s)" % (url, e)
            return False


    def crawl(self):

        """ Main function in the crawling process.  Core algorithm is:

        q <- starting page
        while q not empty:
           url <- q.get()
           if url is new and suitable:
              page <- fetch(url)
              q.put(urls found in page)
           else:
              nothing

        new and suitable means that we don't re-visit URLs we've seen
        already fetched, and user-supplied criteria like maximum
        search depth are checked. """

        q = Queue()
        q.put((self.root, 0))

        while not q.empty():
            this_url, depth = q.get()
            print "retrieving new url ", this_url

            #Non-URL-specific filter: Discard anything over depth limit
            if depth > self.depth_limit:
                continue

            #Apply URL-based filters.
            do_not_follow = [f for f in self.pre_visit_filters if not f(this_url)]

            #Special-case depth 0 (starting URL)
            if depth == 0 and [] != do_not_follow:
                print >> sys.stderr, "Whoops! Starting URL %s rejected by the following filters:", do_not_follow

            #If no filters failed (that is, all passed), process URL
            if [] == do_not_follow:
                try:
                    self.visited_links.add(this_url)
                    self.num_followed += 1
                    page = ArtistFetcher(this_url)
                    page.fetch()
                    print page.out_links()
                    # now we have each artist page, which lists their songs with comment count
                    for link_url in [self._pre_visit_url_condense(l) for l in page.out_links()]:
                        print "\ngoing to artist page ", link_url
                        if link_url not in self.urls_seen:
                            songs = SongFetcher(link_url)
                            songs.fetch()
                            #q.put((link_url, depth+1))
                            self.urls_seen.add(link_url)
                            print songs.out_links()
                            # now we have one artist's list of song titles and URLs, so go grab the html
                            titleURL = songs.out_links()
                            for title in titleURL.keys():
                                songpage_url = self._pre_visit_url_condense(titleURL[title])
                                print "\n !!! going to song page ", title, songpage_url
                                song = PagePuller(songpage_url, title, songs.get_artist())
                                song.fetch()

                        do_not_remember = [f for f in self.out_url_filters if not f(link_url)]
                        if [] == do_not_remember:
                                self.num_links += 1
                                self.urls_remembered.add(link_url)
                                link = Link(this_url, link_url, "href")
                                if link not in self.links_remembered:
                                    self.links_remembered.add(link)
                except Exception, e:
                    print >>sys.stderr, "ERROR: Can't process url '%s' (%s)" % (this_url, e)
                    print format_exc()

class OpaqueDataException (Exception):
    def __init__(self, message, mimetype, url):
        Exception.__init__(self, message)
        self.mimetype=mimetype
        self.url=url


class ArtistFetcher(object):

    """This class retrieves and interprets a web page from songmeanings.net that
        contains links to artist pages. It only collects the artist URL if they
        have a non-zero lyric count."""

    def __init__(self, url):
        self.url = url
        self.out_urls = []

    def __getitem__(self, x):
        return self.out_urls[x]

    def out_links(self):
        return self.out_urls

    def _addHeaders(self, request):
        request.add_header("User-Agent", AGENT)

    def _open(self):
        url = self.url
        try:
            request = urllib2.Request(url)
            handle = urllib2.build_opener()
        except IOError:
            return None
        return (request, handle)

    def fetch(self):
        request, handle = self._open()
        self._addHeaders(request)
        if handle:
            try:
                data=handle.open(request)
                mime_type=data.info().gettype()
                url=data.geturl();
                if mime_type != "text/html":
                    raise OpaqueDataException("Not interested in files of type %s" % mime_type,
                                              mime_type, url)
                content = unicode(data.read(), "utf-8",
                        errors="replace")
                soup = BeautifulSoup(content)
                artistTable = soup.find(id = "listing_artists")
                #tags = soup('a')
                tags = artistTable.find_all(href=re.compile(".*artist\/view.*"))
                #print "found something ", tags
            except urllib2.HTTPError, error:
                if error.code == 404:
                    print >> sys.stderr, "ERROR: %s -> %s" % (error, error.url)
                else:
                    print >> sys.stderr, "ERROR: %s" % error
                tags = []
            except urllib2.URLError, error:
                print >> sys.stderr, "ERROR: %s" % error
                tags = []
            except OpaqueDataException, error:
                print >>sys.stderr, "Skipping %s, has type %s" % (error.url, error.mimetype)
                tags = []
            for tag in tags:
                #check the song count for the artist. it's in the same table row
                if tag.parent.next_sibling.string != "0":
                    print tag.string, " ", tag.parent.next_sibling.string
                    href = tag.get("href")
                    if href is not None:
                        url = urlparse.urljoin(self.url, escape(href))
                        print url, "\n"
                        if url not in self:
                            self.out_urls.append(url)

class SongFetcher(object):

    """This class retrieves and interprets a web page from songmeanings.net that
        contains links to lyric pages. It only collects the song URL if it
        has a non-zero comment count."""

    def __init__(self, url):
        self.url = url
        self.out_urls = {}
        self.artist = "UNKNOWN"

    def __getitem__(self, x):
        return self.out_urls[x]

    def out_links(self):
        return self.out_urls

    def get_artist(self):
        return self.artist

    def _addHeaders(self, request):
        request.add_header("User-Agent", AGENT)

    def _open(self):
        url = self.url
        try:
            request = urllib2.Request(url)
            handle = urllib2.build_opener()
        except IOError:
            return None
        return (request, handle)

    def fetch(self):
        request, handle = self._open()
        self._addHeaders(request)
        if handle:
            try:
                data=handle.open(request)
                mime_type=data.info().gettype()
                url=data.geturl();
                if mime_type != "text/html":
                    raise OpaqueDataException("Not interested in files of type %s" % mime_type,
                                              mime_type, url)
                content = unicode(data.read(), "utf-8", errors="replace")
                soup = BeautifulSoup(content)
                try:
                    #pull out the artist name
                    self.artist = soup.head.title.string.split('|')[2]
                    self.artist = self.artist.strip().replace(' ','_')
                except IndexError, error:
                    #the title was malformed, move on
                    print >> sys.stderr, "WARNING: no artist name %s" % error
                #grab the object holding the song table
                songTable = soup.find(id = "detailed_artists")
                #tags = soup('a')
                tags = songTable.find_all("tr", { "class" : re.compile("row.") })
            except AttributeError, error:
                print >>sys.stderr, "++++++++++++++++++++++ skipping artist page, missing song div.\n %s" % error
                tags = []
            except urllib2.HTTPError, error:
                if error.code == 404:
                    print >> sys.stderr, "ERROR: %s -> %s" % (error, error.url)
                else:
                    print >> sys.stderr, "ERROR: %s" % error
                tags = []
            except urllib2.URLError, error:
                print >> sys.stderr, "ERROR: %s" % error
                tags = []
            except OpaqueDataException, error:
                print >>sys.stderr, "Skipping %s, has type %s" % (error.url, error.mimetype)
                tags = []
            for tag in tags:
                commentCount = int(tag.contents[1].find("a").string)
                 #check the comment count for the song
                if commentCount >= MIN_COMMENTS:
                    songTag = tag.contents[0].find("a")
                    songTitle = songTag.string
                    print songTitle, " ", commentCount
                    href = songTag.get("href")
                    songTitle = songTag.string
                    if href is not None and songTitle is not None:
                        url = urlparse.urljoin(self.url, escape(href))
                        print songTitle,url, " added to song page list \n"
                        self.out_urls[songTitle] = url

class PagePuller(object):

    """This class retrieves and interprets a web page from songmeanings.net that
        contains lyrics  and comments. It grabs the relevant HTML from page one
        and then traverses the rest of the pagination links and appends those.
        These are saved off to a file.
    """

    def __init__(self, url, songtitle, artistname):
        self.url = url
        self.out_urls = []
        self.title = songtitle
        self.artist = artistname

    def __getitem__(self, x):
        return self.out_urls[x]

    def out_links(self):
        return self.out_urls

    def _addHeaders(self, request):
        request.add_header("User-Agent", AGENT)

    def _open(self):
        url = self.url
        try:
            request = urllib2.Request(url)
            handle = urllib2.build_opener()
        except IOError:
            return None
        return (request, handle)

    def fetch(self):
        request, handle = self._open()
        self._addHeaders(request)
        if handle:
            try:
                data=handle.open(request)
                mime_type=data.info().gettype()
                url=data.geturl();
                if mime_type != "text/html":
                    raise OpaqueDataException("Not interested in files of type %s" % mime_type,
                                              mime_type, url)
                content = unicode(data.read(), "utf-8", errors="replace")

                soup = BeautifulSoup(content)
                print soup.prettify()
#                print soup.head.contents[0]
#                artist = soup.find_all(attrs={'href' : re.compile("artist/view/songs")})[0]
#                artistName = artist.string
#                songTitle = artist.parent.next_sibling.string
#
#                commentBlock = soup.find(id="comments_listing")
#                print commentBlock.str()
#                print commentBlock.div['class']
#                f = open(artistName + '_' + songTitle, 'w')
#                f.write(commentBlock)
#                f.close()
                #tags = songTable.find_all("tr", { "class" : re.compile("row.") })
            except AttributeError, error:
                print >>sys.stderr, "++++++++++++++++++++++ skipping song page, missing comments div.\n %s" % error
                tags = []
            except urllib2.HTTPError, error:
                if error.code == 404:
                    print >> sys.stderr, "ERROR: %s -> %s" % (error, error.url)
                else:
                    print >> sys.stderr, "ERROR: %s" % error
                tags = []
            except urllib2.URLError, error:
                print >> sys.stderr, "ERROR: %s" % error
                tags = []
            except OpaqueDataException, error:
                print >>sys.stderr, "Skipping %s, has type %s" % (error.url, error.mimetype)
                tags = []
#            for tag in tags:
#                commentCount = int(tag.contents[1].find("a").string)
#                 #check the comment count for the song
#                if commentCount >= MIN_COMMENTS:
#                    songTag = tag.contents[0].find("a")
#                    songTitle = songTag.string
#                    print songTitle, " ", commentCount
#                    href = songTag.get("href")
#                    if href is not None:
#                        url = urlparse.urljoin(self.url, escape(href))
#                        print url, " added to song page list \n"
#                        if url not in self:
#                            self.out_urls.append(url)

def getLinks(url):
    page = Fetcher(url)
    page.fetch()
    for i, url in enumerate(page):
        print "%d. %s" % (i, url)

def parse_options():
    """parse_options() -> opts, args

    Parse any command-line options given returning both
    the parsed options and arguments.
    """

    parser = optparse.OptionParser(usage=USAGE, version=VERSION)

    parser.add_option("-q", "--quiet",
            action="store_true", default=False, dest="quiet",
            help="Enable quiet mode")

    parser.add_option("-l", "--links",
            action="store_true", default=False, dest="links",
            help="Get links for specified url only")

    parser.add_option("-d", "--depth",
            action="store", type="int", default=30, dest="depth_limit",
            help="Maximum depth to traverse")

    parser.add_option("-c", "--confine",
            action="store", type="string", dest="confine",
            help="Confine crawl to specified prefix")

    parser.add_option("-x", "--exclude", action="append", type="string",
                      dest="exclude", default=[], help="Exclude URLs by prefix")

    parser.add_option("-L", "--show-links", action="store_true", default=False,
                      dest="out_links", help="Output links found")

    parser.add_option("-u", "--show-urls", action="store_true", default=False,
                      dest="out_urls", help="Output URLs found")

    parser.add_option("-D", "--dot", action="store_true", default=False,
                      dest="out_dot", help="Output Graphviz dot file")



    opts, args = parser.parse_args()

    if len(args) < 1:
        parser.print_help(sys.stderr)
        raise SystemExit, 1

    if opts.out_links and opts.out_urls:
        parser.print_help(sys.stderr)
        parser.error("options -L and -u are mutually exclusive")

    return opts, args

class DotWriter:

    """ Formats a collection of Link objects as a Graphviz (Dot)
    graph.  Mostly, this means creating a node for each URL with a
    name which Graphviz will accept, and declaring links between those
    nodes."""

    def __init__ (self):
        self.node_alias = {}

    def _safe_alias(self, url, silent=False):

        """Translate URLs into unique strings guaranteed to be safe as
        node names in the Graphviz language.  Currently, that's based
        on the md5 digest, in hexadecimal."""

        if url in self.node_alias:
            return self.node_alias[url]
        else:
            m = hashlib.md5()
            m.update(url)
            name = "N"+m.hexdigest()
            self.node_alias[url]=name
            if not silent:
                print "\t%s [label=\"%s\"];" % (name, url)
            return name


    def asDot(self, links):

        """ Render a collection of Link objects as a Dot graph"""

        print "digraph Crawl {"
        print "\t edge [K=0.2, len=0.1];"
        for l in links:
            print "\t" + self._safe_alias(l.src) + " -> " + self._safe_alias(l.dst) + ";"
        print  "}"




def main():
    opts, args = parse_options()

    url = args[0]

    if opts.links:
        getLinks(url)
        raise SystemExit, 0

    depth_limit = opts.depth_limit
    confine_prefix=opts.confine
    exclude=opts.exclude

    sTime = time.time()

    print >> sys.stderr,  "Crawling %s (Max Depth: %d)" % (url, depth_limit)
    crawler = Crawler(url, depth_limit, confine_prefix, exclude)
    crawler.crawl()
#    print AGENT
    if opts.out_urls:
        print "\n".join(crawler.urls_seen)

    if opts.out_links:
        print "\n".join([str(l) for l in crawler.links_remembered])

    if opts.out_dot:
        d = DotWriter()
        d.asDot(crawler.links_remembered)

    eTime = time.time()
    tTime = eTime - sTime

    print >> sys.stderr, "Found:    %d" % crawler.num_links
    print >> sys.stderr, "Followed: %d" % crawler.num_followed
    print >> sys.stderr, "Stats:    (%d/s after %0.2fs)" % (
            int(math.ceil(float(crawler.num_links) / tTime)), tTime)

if __name__ == "__main__":
    main()
