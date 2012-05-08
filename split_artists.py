#!/usr/bin/python

import os
import sys

def make_condor_job(executable, args, out = "_stdout", err = "_error", log = "_log", mem = 2):
    """Contains the skeletons of a condor job running in the vanilla universe. 
    First argument is for executable, second for arguments, 
    out for output (defaults to "_out"), err for error (defaults to "_err"), log for log (defaults to "_log"), 
    and mem is the number of GB of memory needed (defaults to 2)."""
    # if args is sent as ["1", "2", "3"] then convert it to "1 2 3"
    if type(args) == list:
        args = " ".join(args)
    condor = []
    # append universe
    condor.append("universe = vanilla")
    # append executable
    condor.append("executable = " + executable)
    # append arguments
    condor.append("arguments = " + '"' + args + '"')
    # append output, error, and log paths
    condor.append("output = " + out)
    condor.append("error = " + err)
    condor.append("log = " + log)
    #condor.append("request_memory = " + str(mem) + "*1024")
    condor.append("transfer_executable = false")
    condor.append("queue")

    return "\n".join(condor) + "\n"


def main():
	
	if len(sys.argv) < 3:
		print "Give me a song URL and an output dir"
		raise SystemExit(1)
	
	
	dagstring = []
	dag = open("dagfile.dag", 'w')
	
	# number
	fullURL = sys.argv[1] + "number"
	fulloutput = os.path.join(sys.argv[2], "number")
	print fullURL, fulloutput
	writeme = make_condor_job( executable = "/opt/python-2.7/bin/python2.7", args = "crawler.py " + fullURL + " " + fulloutput)
	f = open("number.cmd", 'w')
	f.write(writeme)
	f.close
	dag.write("JOB\tnumber\tnumber.cmd\n")
	dagstring.append("PARENT Z CHILD number")
	
	# alphabet
	for code in range(ord('A'), ord('Z')+1):
		fullURL = sys.argv[1] + chr(code)
		fulloutput = os.path.join(sys.argv[2], chr(code))
		print fullURL, fulloutput
		writeme = make_condor_job( executable = "/opt/python-2.7/bin/python2.7", args = "crawler.py " + fullURL + " " + fulloutput)
		#write the condor script for each letter
		f = open(chr(code) + ".cmd", 'w')
		f.write(writeme)
		f.close
		# add a dependency to the DAG file. I don't want these to all run at the same time
		dag.write("JOB\t" + chr(code) + "\t" + chr(code) + ".cmd" + "\n")
		if code < ord('Z'):
			dagstring.append("PARENT " + chr(code) + " CHILD " + chr(code+1))
	
	dag_dependencies = "\n".join(dagstring) + "\n"
	dag.write(dag_dependencies)
	dag.close()


if __name__ == "__main__":
    main()
