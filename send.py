import sys
from pyNifi import pyNifi

if(len(sys.argv)<4):
	print "usage: python send.py [nifi host] [nifi web api port] [filename] [portname]"
else:
	nifi = pyNifi(sys.argv[1],sys.argv[2])
	attributes={'flowatrributes':'can be sent as dicts'}
	nifi.sendFile(sys.argv[3],sys.argv[4],attributes)
