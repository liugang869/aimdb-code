#!/usr/bin/python

import os,sys
import subprocess
import stat

print 'compile...'
subprocess.call('make')
files =  os.listdir('./debug/')

print '\nrun test...\n'
for exe in files:
	run = './debug/'+exe
	if os.access (run, os.X_OK):
		cmd = run
		print exe,'...'
		process = subprocess.Popen (cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		if 'test pass!' in process.stdout.read():
			print exe,'pass!'
		else:
			print exe,'fail!'
subprocess.Popen ('rm tmp.log', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
print '\ntests pass!\n'
