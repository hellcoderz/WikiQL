f = open('../reverb_clueweb_tuples-1.1.txt','r')
def print_words(words):
	st = ""
	for i in range(len(words)):
		if i == 0:
			st = words[i]
		else:
			st = st + "\t" + words[i]
	print st

for line in f:
	#line = f.readline()
	words = line.split("\t")
	if words[2] == words[5]:
		del words[4:]
		print_words(words)
	else:
		words[2] = words[2] + "---" + words[5]
		del words[4:]
		print_words(words)