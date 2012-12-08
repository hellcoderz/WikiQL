import re
import sys

def parse(st):
	#relation
	st1 = "a=arg1,rel,arg2 AS city,population"
	pat_relation = "([a-z])[\s]*=[\s]*(\w+|-)[,](\w+|-)[,](\w+|-)[\s]+AS[\s]+(\w+)[,](\w+)[\s]*"

	m = re.search(pat_relation,st)
	if m:
		args = m.groups()
		return "val "+args[0].strip()+' = relation(sc,"'+args[1].strip()+","+args[2].strip()+","+args[3].strip()+'",input)',args[0].strip()


	#a=COUNT b
	st2 = "a=COUNT b"
	pat_count = "([a-z])[\s]*=[\s]*COUNT[\s]+([a-z])[\s]*"
	m = re.search(pat_count,st)
	if m:
		args = m.groups()
		return "val "+args[0].strip()+" = "+args[1].strip()+".count()",args[0].strip()
	


	#SUM
	st3 = "a=SUM b BY population"
	pat_sum = "([a-z])[\s]*=[\s]*SUM[\s]+([a-z])[\s]+BY[\s]+(\w+)[\s]*"
	m = re.search(pat_sum,st)
	if m:
		args = m.groups()
		return "val "+args[0].strip()+" = sum("+args[1].strip()+","+args[2].strip()+")",args[0].strip()
	


	#JOIN
	st4 = "a=JOIN b WITH c"
	pat_join = "([a-z])[\s]*=[\s]*JOIN[\s]+([a-z])[\s]+WITH[\s]+([a-z])[\s]*"
	m = re.search(pat_join,st)
	if m:
		args = m.groups()
		return "val "+args[0].strip()+" = "+args[1].strip()+".join("+args[2].strip()+")",args[0].strip()
	

	#MAX
	st5 = "a=MAX b BY population"
	pat_max = "([a-z])[\s]*=[\s]*MAX[\s]+([a-z])[\s]+BY[\s]+(\w+)[\s]*"
	m = re.search(pat_max,st)
	if m:
		args = m.groups()
		return "val "+args[0].strip()+" = max("+args[1].strip()+","+args[2].strip()+")",args[0].strip()

	#MIN
	st6 = "a=MIN b BY population"
	pat_min = "([a-z])[\s]*=[\s]*MIN[\s]+([a-z])[\s]+BY[\s]+(\w+)[\s]*"
	m = re.search(pat_min,st)
	if m:
		args = m.groups()
		return "val "+args[0].strip()+" = min("+args[1].strip()+","+args[2].strip()+")",args[0].strip()
	

	#FILTER
	st7 = "a=FILTER b BY population WHEN str"
	pat_filter = "([a-z])[\s]*=[\s]*FILTER[\s]+([a-z])[\s]+BY[\s]+(\w+)[\s]+WHEN[\s]+(\w+)[\s]*"
	m = re.search(pat_filter,st)
	if m:
		args = m.groups()
		return "val "+args[0].strip()+" = filter("+args[1].strip()+","+args[2].strip()+","+args[3].strip()+")",args[0].strip()
	

	#NOT FILTER
	st8 = "a=FILTER b BY population WHEN NOT str"
	pat_filter_not = "([a-z])[\s]*=[\s]*FILTER[\s]+([a-z])[\s]+BY[\s]+(\w+)[\s]+WHEN NOT[\s]+(\w+)[\s]*"
	m = re.search(pat_filter_not,st)
	if m:
		args = m.groups()
		return "val "+args[0].strip()+" = not_filter("+args[1].strip()+","+args[2].strip()+","+args[3].strip()+")",args[0].strip()

    #NOT FILTER
	st9 = "SHOW a"
	pat_show = "SHOW[\s]+([a-z])"
	m = re.search(pat_show,st)
	if m:
		args = m.groups()
		return args[0].strip()+".saveAsTextFile(output_file)","-"
	
	return (),()




def parse_file(file_name):
	variables = []
	for line in open(file_name+".wql",'r'):
		out,next_input = parse(line)
		if next_input in variables:
			print "ERROR: variable "+next_input+" already used"
		else:
			print out
			variables.append(next_input)

parse_file(sys.argv[1])