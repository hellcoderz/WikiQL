import re
import sys
#print sys.argv[2]
f = open(sys.argv[2],'r')
arg = sys.argv[1]
args = arg.split(",")

arg1 = args[0]
rel = args[1]
arg2 = args[2]
#print arg1,rel,arg2
a1 = 0
r = 0
a2 = 0


m = re.search("^type:(.*)",arg1)
if m:
	arg1 = "<type="+m.group(1).lower()+">(.*)</type>"
	a1 = 1
else:
	arg1 = arg1.strip().lower()

m = re.search("^type:(.*)",arg2)
if m:
	arg2 = "<type="+m.group(1).lower()+">(.*)</type>"
	a1 = 1
else:
	arg2 = arg2.strip().lower()

rel = rel.strip().lower()

#print arg1,rel,arg2

for line in f:
	words = line.split("\t")
	#print "INPUT: ",words
	arg1_pat = arg1
	rel_pat = rel
	arg2_pat = arg2
	if len(words) >= 3:
		m1 = re.search(arg1_pat,words[1].lower())
		m2 = re.search(arg2_pat,words[3].lower())
		if m1 and re.search(rel_pat,words[2].lower()) and m2:
			print words[1]+","+words[3]