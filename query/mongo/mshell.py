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
	sd = inp.split("<-->")
	sw = sd[0].split("+")
	dw = sd[1].split("+")
	print "Lets find how",sw[0],"("+sw[1]+")","and",dw[0],"("+dw[1]+")","are related!!"
	for val in query_relation(posts,sw[0].strip(),sw[1].strip(),dw[0].strip(),dw[1].strip(),N):
		if not val == "DONE":
			print "RELATION:",val
		else:
			print "We can find these relations only."