import re
import sys
import pexpect

#single static assignments variable
ssa = []
#counters variables produced by COUNT operation
counters = []
#variable having key value pairs
mrvar = []
#variable for assigning unique values to SSA
C = 0
middle = ""
lastpartofquery = """.saveAsTextFile("out")
    					System.exit(0)
  					}
		}
"""

def parse(st,ssa,counters,mrvar,C,lineno):
	#query structured
	st1 = "A=AND:type;category+OR:type;category+OTHER:key;value"
	pat_S = "([A-Z])[\s]*=[\s]*AND:[\s]*(.*)[\s]*\+[\s]*OR:[\s]*(.*)[\s]*\+[\s]*OTHER:[\s]*(.*)[\s]*"

	m = re.search(pat_S,st)
	if m:
		AND = True
		OR = True
		OTHER = True
		qAND = ""
		qOR = ""
		qOTHER = ""
		args = m.groups()

		if args[0].strip() in ssa or args[0].strip() in counters or args[0].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable can't be assigned twice"
			sys.exit()

		ssa.append(args[0].strip())	
		mrvar.append(args[0].strip())

		if args[1].strip() == "-":
			ANDtype = "-"
			ANDcat = "-"
			AND = False
		else:
			spAND = args[1].split(";")
			ANDtype = spAND[0].strip()
			ANDcat = spAND[1].strip()

		if args[2].strip() == "-":
			ORtype = "-"
			ORcat = "-"
			OR = False
		else:
			spOR = args[2].split(";")
			ORtype = spOR[0].strip()
			ORcat = spOR[1].strip()

		if args[3].strip() == "-":
			OTHERtype = "-"
			OTHERcat = "-"
			OTHER = False
		else:
			spOTHER = args[3].split(";")
			OTHERkey = spOTHER[0].strip()
			OTHERvalue = spOTHER[1].strip()

		if AND:
			qAND = '.filter(line => findByTypeAndCategoryAND(line,"'+ANDtype+'","'+ANDcat+'") )'
		if OR:
			qOR = '.filter(line => findByTypeAndCategoryAND(line,"'+ORtype+'","'+ORcat+'") )'
		if OTHER:
			qOTHER = '.filter(line => findOtherAND(line,"'+OTHERkey+'","'+OTHERvalue+'") )'

		out = "XZS"
		last = args[0].strip()

		if AND and not OR and not OTHER:
			return "val "+out+" = input" + qAND +"\n"+"val "+last+" = XZS.map(line => mapstokey(line))"
		elif AND and OR and not OTHER:
			n = C
			C = C + 1
			return "val out"+n+" = input" + qOR +"\n" + "val "+out+" = out"+ n  + qAND +"\n"+"val "+last+" = XZS.map(line => mapstokey(line))"
		elif AND and OR and OTHER:
			n = C
			n1 = C + 1
			C = C + 2
			return "val out"+n1+" = input" + qOR +"\n" + "val out"+ n +" = out"+ n1 + qAND + "\n" + "val "+out+" = out"+n+ + qOTHER +"\n"+"val "+last+" = XZS.map(line => mapstokey(line))"
		elif not AND and OR and not OTHER:
			return "val "+out+" = input" + qOR +"\n"+"val "+last+" = XZS.map(line => mapstokey(line))"
		elif not AND and OR and OTHER:
			return "val "+out+" = input" + qOTHER +"\n"+"val "+last+" = XZS.map(line => mapstokey(line))"
		elif AND and not OR and OTHER:
			n = C
			C = C + 1
			return "val out"+n+" = input" + qAND + "\n" + "val "+out+" = out"+ n + qOTHER +"\n"+"val "+last+" = XZS.map(line => mapstokey(line))"
		return "0"


	#query unstructured
	st2 = "A=ARG1:-------+REL:------+ARG2:---------"
	pat_US = "([A-Z])[\s]*=[\s]*ARG1:[\s]*(.*)[\s]*\+[\s]*REL:[\s]*(.*)[\s]*\+[\s]*ARG2:[\s]*(.*)[\s]*"
	m = re.search(pat_US,st)
	if m:
		args = m.groups()
		if args[0].strip() in ssa or args[0].strip() in counters or args[0].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable can't be assigned twice"
			sys.exit()
		ssa.append(args[0].strip())	
		mrvar.append(args[0].strip())

		return "val "+args[0].strip()+' = relation("'+args[1].strip()+","+args[2].strip()+","+args[3].strip()+'",inputUS)'
	


	#COUNT
	st3 = "C=COUNT A"
	pat_count = "([A-Z])[\s]*=[\s]*COUNT[\s]+([A-Z])[\s]*"
	m = re.search(pat_count,st)
	if m:
		args = m.groups()
		if args[0].strip() in ssa or args[0].strip() in counters or args[0].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable can't be assigned twice"
			sys.exit()
		if args[1].strip() in counters:
			print "AT LINE "+str(lineno)+" ERROR: Variable does not contain key/value pair"
			sys.exit()
		if not args[1].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable not created"
			sys.exit()

		ssa.append(args[0].strip())	
		counters.append(args[0].strip())
		return "val "+args[0].strip()+" = "+args[1].strip()+".count()"
	


	#JOIN
	st4 = "D=JOIN A B"
	pat_join = "([A-Z])[\s]*=[\s]*JOIN[\s]+([A-Z])[\s]+([A-Z])[\s]*"
	m = re.search(pat_join,st)
	if m:
		args = m.groups()
		if args[0].strip() in ssa or args[0].strip() in counters or args[0].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable can't be assigned twice"
			sys.exit()
		if args[1].strip() in counters and args[2].strip() in counters:
			print "AT LINE "+str(lineno)+" ERROR: Variable does not contain key/value pair"
			sys.exit()
		if not args[1].strip() in mrvar or not args[2].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variables not created"
			sys.exit()

		ssa.append(args[0].strip())	
		mrvar.append(args[0].strip())
		return "val "+args[0].strip()+" = "+args[1].strip()+".join("+args[2].strip()+")"
	

	#UNION
	st5 = "A=UNION A B"
	pat_union = "([A-Z])[\s]*=[\s]*UNION[\s]+([A-Z])[\s]+([A-Z])[\s]*"
	m = re.search(pat_union,st)
	if m:
		args = m.groups()
		if args[0].strip() in ssa or args[0].strip() in counters or args[0].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable can't be assigned twice"
			sys.exit()
		if args[1].strip() in counters and args[2].strip() in counters:
			print "AT LINE "+str(lineno)+" ERROR: Variable does not contain key/value pair"
			sys.exit()
		if not args[1].strip() in mrvar or not args[2].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variables not created"
			sys.exit()

		ssa.append(args[0].strip())	
		mrvar.append(args[0].strip())
		return "val "+args[0].strip()+" = "+args[1].strip()+".union("+args[2].strip()+")"

	#GROUP
	st6 = "A=GROUP B"
	pat_group = "([A-Z])[\s]*=[\s]*GROUP[\s]+([A-Z])[\s]*"
	m = re.search(pat_group,st)
	if m:
		args = m.groups()
		if args[0].strip() in ssa or args[0].strip() in counters or args[0].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable can't be assigned twice"
			sys.exit()
		if args[1].strip() in counters:
			print "AT LINE "+str(lineno)+" ERROR: Variable does not contain key/value pair"
			sys.exit()
		if not args[1].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable not created"
			sys.exit()

		ssa.append(args[0].strip())	
		mrvar.append(args[0].strip())
		return "val "+args[0].strip()+" = "+args[1].strip()+".groupByKey().mapValues(values => map3(values))"


	#REDUCE
	st6 = "A=REDUCE B"
	pat_reduce = "([A-Z])[\s]*=[\s]*REDUCE[\s]+([A-Z])[\s]*"
	m = re.search(pat_reduce,st)
	if m:
		args = m.groups()
		if args[0].strip() in ssa or args[0].strip() in counters or args[0].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable can't be assigned twice"
			sys.exit()
		if args[1].strip() in counters:
			print "AT LINE "+str(lineno)+" ERROR: Variable does not contain key/value pair"
			sys.exit()
		if not args[1].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable not created"
			sys.exit()

		ssa.append(args[0].strip())	
		mrvar.append(args[0].strip())
		return "val "+args[0].strip()+" = "+args[1].strip()+".reduceByKey(_+'---'+_)"



	#FILTERBYVALUE
	st8 = "A=FILTER B BY -------"
	pat_filterbyvalue = "([A-Z])[\s]*=[\s]*FILTER[\s]+([A-Z])[\s]+BY[\s]+(.+)[\s]*"
	m = re.search(pat_filterbyvalue,st)
	if m:
		args = m.groups()
		if args[0].strip() in ssa or args[0].strip() in counters or args[0].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable can't be assigned twice"
			sys.exit()
		if args[1].strip() in counters and args[1].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable does not contain key/value pair"
			sys.exit()
		if not args[1].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable not created"
			sys.exit()

		ssa.append(args[0].strip())
		return "val "+args[0].strip()+" = "+args[1].strip()+".filter(line => line.contains("+args[2].strip()+"))"

   
	#NOTFILTERBYVALUE
	st9 = "A=NOTFILTER B BY -------"
	pat_filterbyvalue = "([A-Z])[\s]*=[\s]*NOTFILTER[\s]+([A-Z])[\s]+BY[\s]+(.+)[\s]*"
	m = re.search(pat_notfilterbyvalue,st)
	if m:
		args = m.groups()
		if args[0].strip() in ssa or args[0].strip() in counters or args[0].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable can't be assigned twice"
			sys.exit()
		if args[1].strip() in counters and args[1].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable does not contain key/value pair"
			sys.exit()
		if not args[1].strip() in mrvar:
			print "AT LINE "+str(lineno)+" ERROR: Variable not created"
			sys.exit()

		ssa.append(args[0].strip())
		return "val "+args[0].strip()+" = "+args[1].strip()+".filter(line => !line.contains("+args[2].strip()+"))"
	
	return "0"




def parse_file(file_name):
	middle = ""
	#single static assignments variable
	ssa = []
	#counters variables produced by COUNT operation
	counters = []
	#variable having key value pairs
	mrvar = []
	#variable for assigning unique values to SSA
	C = 0

	lastpartofquery = """.saveAsTextFile("out")
    System.exit(0)
  	}
	}
	"""

	f = open(file_name+".wql","r")
	lineno = 1
	for line in f:
		pline = parse(line.strip(),ssa,counters,mrvar,C,lineno)
		if pline == "0":
			print "AT LINE "+str(lineno)+" ERROR: wrong syntax"
		else:
			middle = middle + pline + "\n"
		lineno = lineno + 1

	lastpartofquery = mrvar[-1] + lastpartofquery
	print middle
	print lastpartofquery
	return middle,lastpartofquery

middle,lastpartofquery = parse_file("test")
r = open("main_query_part1.scala",'r')
part1 = r.read()
wholefile = part1+middle+lastpartofquery
w = open("main.scala","w")
w.write(wholefile)

if sys.argv[1] == "-r":		#if mode is RUN then run the shell script to compile and run the scala program
	os.system("./ec2_runmain.sh")		#run the shell script
else if sys.argv[1] == "-nr":		#mode to just output the parsed query
	print middle
	print lastpartofquery
else:
	print "TRY AGAIN"


#TESTS
"""
print parse("A=AND: Actor:AdultActor;1965 births + OR:- + OTHER:-",ssa,counters,mrvar,C,1)
print parse("B=ARG1: type:drug + REL: kill|kills|affective|against + ARG2: bacteria",ssa,counters,mrvar,C,2)
print parse("E=UNION A B",ssa,counters,mrvar,C,3)
print parse("F=GROUP E",ssa,counters,mrvar,C,4)
print parse("D=COUNT E",ssa,counters,mrvar,C,5)

lastpartofquery = mrvar[-1] + lastpartofquery
print lastpartofquery
"""