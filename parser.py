import re

def parse(st):
	#relation
	pat_relation = "([a-z])[\s]*=[\s]*(\w+|-)[,](\w+|-)[,](\w+|-)[\s]+AS[\s]+(\w+)[,](\w+)[\s]*"

	m = re.search(pat_relation,st)
	if m:
		return m.groups()


	#a=COUNT b
	st2 = "a=COUNT b"
	pat_count = "([a-z])[\s]*=[\s]*COUNT[\s]+([a-z])[\s]*"
	m = re.search(pat_count,st)
	if m:
		return m.groups()
	


	#SUM
	st3 = "a=SUM b BY population"
	pat_sum = "([a-z])[\s]*=[\s]*SUM[\s]+([a-z])[\s]+BY[\s]+(\w+)[\s]*"
	m = re.search(pat_sum,st)
	if m:
		return m.groups()
	


	#JOIN
	st4 = "a=JOIN b WITH c"
	pat_join = "([a-z])[\s]*=[\s]*JOIN[\s]+([a-z])[\s]+WITH[\s]+([a-z])[\s]*"
	m = re.search(pat_join,st)
	if m:
		return m.groups()
	

	#MAX
	st5 = "a=MAX b BY population"
	pat_max = "([a-z])[\s]*=[\s]*MAX[\s]+([a-z])[\s]+BY[\s]+(\w+)[\s]*"
	m = re.search(pat_max,st)
	if m:
		return m.groups()
	

	#MIN
	st6 = "a=MIN b BY population"
	pat_min = "([a-z])[\s]*=[\s]*MIN[\s]+([a-z])[\s]+BY[\s]+(\w+)[\s]*"
	m = re.search(pat_min,st)
	if m:
		return m.groups()
	

	#FILTER
	st7 = "a=FILTER b BY population WHEN str"
	pat_filter = "([a-z])[\s]*=[\s]*FILTER[\s]+([a-z])[\s]+BY[\s]+(\w+)[\s]+WHEN[\s]+(\w+)[\s]*"
	m = re.search(pat_filter,st)
	if m:
		return m.groups()
	

	#NOT FILTER
	st8 = "a=FILTER b BY population WHEN NOT str"
	pat_filter_not = "([a-z])[\s]*=[\s]*FILTER[\s]+([a-z])[\s]+BY[\s]+(\w+)[\s]+WHEN NOT[\s]+(\w+)[\s]*"
	m = re.search(pat_filter_not,st)
	if m:
		return m.groups()
	
	return ()

print parse(raw_input())