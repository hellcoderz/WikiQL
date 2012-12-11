from mongo import init,query_relation,find_new
import sys
import pymongo
from pymongo import Connection
import re

conn = Connection('localhost',8989)
db = conn.mydb
posts = db.posts
N = 5

while True:

	inp = raw_input("Query>>")
	if inp == "quit":
		sys.exit()
	sw = inp.split("+")
	
	#print "Lets find ",sw[0],"("+sw[1]+")","and",dw[0],"("+dw[1]+")","are related!!"
	for val in find_new(posts,sw[0].strip(),sw[1].strip(),sw[2].strip(),sw[3].strip(),sw[4].strip()):
		if not val == "DONE":
			print "RESULT:",val
		else:
			print "We can find these relations only."