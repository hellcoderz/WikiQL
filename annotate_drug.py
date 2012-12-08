import re
import sys

file_store = open(sys.argv[1],'r')
names = []
for name in file_store:
	names.append(name[:len(name)-1].lower())

data = open(sys.argv[2],'r')

annotate_text = sys.argv[3].upper()
type1 = ""
type2 = ""
word1 = ""
word2 = ""
#print names
for line in data:
	flag = 0
	words = line.split("\t")
	#print "==================",words[1].strip().lower(),words[3].strip().lower()

	
	if words[1].strip().lower().find("<type=") > -1:
		m1 = re.search("<type=(.*)>(.*)</type>",words[1])
		if m1:
			flag = 1
			word1 = m1.group(2).strip()
			type1 = m1.group(1).strip()+" | "
			if word1 in names:
				#print "wor1",word1
				words[1] = "<type= "+type1+annotate_text+" > " + word1 + " </type>"

	elif words[1].strip().lower() in names:
		word1 = words[1].strip()
		flag = 1
		if words[1].strip().lower() == word1.lower():
			words[1] = "<type= "+type1+annotate_text+" > " + word1 + " </type>"


	if words[3].strip().lower().find("<type=") > -1:
		#print "words[1] with type"
		m2 = re.search("<type=(.*)>(.*)</type>",words[3])
		if m2:
			flag = 1
			word2 = m2.group(2).strip().replace('\n','')
			type2 = m2.group(1).strip()+" | "
			if word2 in names:
				words[3] = "<type= "+type2+annotate_text+" > " + word2 + " </type>"
			

	elif words[3].strip().lower() in names:
		word2 = words[3].strip().replace('\n','')
		flag = 1
		if words[3].strip().lower() == word2.lower():	
			words[3] = "<type= "+type2+annotate_text+" > " + word2 + " </type>"

	if flag ==  1:
		print("\t".join(words).replace('\n',''))
	else:
		print(line.replace('\n',''))