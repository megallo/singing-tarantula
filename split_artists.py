#!/usr/bin/python

import os
import sys

def main():
	
	if len(sys.argv) < 3:
		print "Give me a song URL and an output dir"
		raise SystemExit(1)

	for code in range(ord('A'), ord('Z')+1):
		fullURL = sys.argv[1] + chr(code)
		fulloutput = os.path.join(sys.argv[2], chr(code))
		print fullURL, fulloutput


if __name__ == "__main__":
    main()
