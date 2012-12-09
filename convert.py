import re
import sys

f = open(sys.argv[1],'r')
for line in f:
	hm = {}
	keyvalue = line.split(",")
	key = keyvalue[0]
	value = keyvalue[1]
	line = line.replace("\n","")
	values = value.split("\t")
	for val in values:
		if len(val.strip()) > 2:
			#print "val",val
			keyvalue1 = val.split(":")
			if len(keyvalue1) > 1:
				key1 = keyvalue1[0].strip().lower()
				#print "key1",key1
				value1 = keyvalue1[1]

				if not hm.has_key(key1):
					hm[key1] = []

				if value1.find(";") > -1:
					for val1 in value1.split(";"):
						hm[key1].append(val1.strip())
				else:
					hm[key1].append(value1.strip())
	sys.stdout.write(key[1:]+"")
	valuek = ""
	for k in hm.iterkeys():
		valuek = valuek + "\t"+k
		for v in hm[k]:
			valuek = valuek + "<>"+v
	valuek = valuek.replace(")","")
	sys.stdout.write(valuek+"\n")
	

