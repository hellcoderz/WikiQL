import re
import sys

file_store = open(sys.argv[1],'r')
names = []
names1 = []
for name in file_store:
	names.append(name[:len(name)-1].lower())
print len(names)

names1 = names[:]
print len(names1)
#names = ["shahrukh khan"]


data = open(sys.argv[2],'r')

annotate_text = sys.argv[3].upper()
type1 = ""
type2 = ""
word1 = ""
word2 = ""



def annotate_arg1(names,words,annsotate_text):
	done1 = 0
	done2 = 0
	for name in names:
		#print names
			
		name = name.strip().lower()

		m = re.search("<type=(.*)>(.*)</type>",words[1].lower())
		if m and done1 == 0:
			#print "typed"
			word1 = m.group(2)
			type1 = m.group(1)
			#print "WORD=",word1
			if re.search(word1.lower().strip(),name):
				words[1] = "<type="+type1+"|"+annotate_text+">"+word1+"</type>"
				#print words[1]
				done1 = 1
		
		elif done1 == 0:	
			#print name,words[1]
			name = name.replace("?"," ")
			name = name.replace("*"," ")
			name = name .replace(".","\.")
			name = name.replace("+"," ")
			name = name.replace("["," ")
			name = name.replace("]"," ")
			if not re.search("[\(]|[\)]" , name):
				m = re.search(name,words[1].lower())
				if m:
					#print "untyped:arg1"
					words[1] = "<type="+annotate_text+">"+name+"</type>"
					done1 = 1


		m = re.search("<type=(.*)>(.*)</type>",words[3].lower())
		if m and done2 == 0:
			
			word1 = m.group(2)
			type1 = m.group(1)
			#print "WORD=",word1
			if len(name)> 0 and re.search(word1.lower().strip(),name):
				#print "typed:arg2"
				words[3] = "<type="+type1+"|"+annotate_text+">"+word1+"</type>"
				#print words[1]
				done2 == 1
		
		elif done2 == 0:	
			name = name.replace("?"," ")
			name = name .replace(".","\.")
			name = name.replace("*"," ")
			name = name.replace("+"," ")
			name = name.replace("["," ")
			name = name.replace("]"," ")
			if len(name) > 0 and not re.search("[\(]|[\)]" , name):
				#print "arg2-name",name
				m = re.match(name,words[3].lower())
				if m:
					#print "untyped:arg2"
					words[3] = "<type="+annotate_text+">"+name+"</type>"
					done2 = 1
		if done1 == 1 and done2 == 1:
			return 1

	if done1 == 1 or done2 == 1:
		return 1
	return 0


def annotate_arg2(names1,words,annotate_text):
	for name in names:
		#print names
			
		name = name.strip().lower()


		m = re.search("<type=(.*)>(.*)</type>",words[3].lower())
		if m:
			print "typed:arg2"
			word1 = m.group(2)
			type1 = m.group(1)
			#print "WORD=",word1
			if re.search(word1.lower().strip(),name):
				words[3] = "<type="+type1+"|"+annotate_text+">"+word1+"</type>"
				#print words[1]
				return 1
		
		else:	
			
			if not re.search("\(" , name) and len(name) > 0:
				m = re.search(name,words[3].lower())
				if m:
					print name,words[3]
					print "untyped:arg2"
					words[3] = "<type="+annotate_text+">"+name+"</type>"
					return 1
	return 0

for line in data:
	
	words = line.split("\t")
	print words
	flag = 0
	flag = annotate_arg1(names,words,annotate_text)	
	words[3] = words[3].replace('\n','')

	#flag = annotate_arg2(names,words,annotate_text)	

	if flag ==  1:
		print "\t".join(words)
	#else:
	#	print line