import pymongo
from pymongo import Connection
import re
import sys

conn = None
db = None
wiki = None

def init(port=8989):
	conn = Connection('localhost',port)
	db = conn.mydb
	txt = db.txt
	wiki = db.posts
	return db,conn,wiki
	

def test(txt,posts):
	text = [{"key":"vikrant","friends":["rishi","manas"],"names":["yogi"]},{"key":"rishi","friends":["vikrant","murali","madhav"],"nam":["yani"]},{"key":"manas","friends":["murali","madhav"]},{"key":"yogi","friends":["unk","madhav"],"na":["hello"]},{"key":"murali","friends":["yogi","madhav"],"na":["hello"]}]
    	txt.remove()
    	txt.insert(text)
	groupall("type:film<>category:indian film")
	txt.find_one()

	for i in find_new(txt,"-","type:Actor","children","-","type:Actor"):
   		print i

   	for p in txt.find({"name":"vikrant"}):
    		for v in p['friends']:
        		for k in txt.find({"name":v}):
            			if "madhav" in k['friends']:
                			print "vikrant","friends with",k["name"],"friends with","madhav"

    	hm = {}
	for i in find_relation(txt,"vikrant","-","madhav","-",5,[]): 
    		if i.find("-") > -1:
        		pass
    		else:
        		if hm.has_key(i):
        	    		pass
        		else:
            			hm[i] = 1
            			print i
	hm = {}
	for i in find_relation(posts,"Sunny Deol","type:Actor","Bobby Deol","type:Actor",5,[]):
    		if i.find("-") > -1:
        		pass
    		else:
        		if hm.has_key(i):
            			pass
        		else:
            			hm[i] = 1
            			print i

   	hm = {}
	for i in find("vikrant","madhav",5): 
    		if i.find("-") > -1:
    	    		pass
    		else:
        		if hm.has_key(i):
            			pass
        		else:
            			hm[i] = 1
            			print i

   	for i in find_new(posts,"-","type:Film","starring","-","type:Actor<>"):
    		print i

    	for i in find_new(posts,"Don","type:Film","starring","-","type:Actor"):
    		print i
    	posts.find_one({"type":"Film","key":{"$regex":".*Hangover.*"}})
    	posts.find_one({"key":{"$regex":"(^Sunny Deol$)"}})


def test_update(posts):
	#add actor
	posts.update({"category":{"$regex":"(.*[a|A]ctor.*)"},"type":{"$nin":["Actor"]}},{"$push":{u'type':u"Actor"}},multi=True)   

def uni(st):
    return unicode(st,'utf-8', 'replace')

def insert(posts):
    f = open("xae","r")
    i=0
    count = 0
    post = []
    for line in f:         
        st={}
        words = line.split("\t")
        st["key"] = uni(words[0])
        #print "len(words)",len(words)
        for j in range(1,len(words)):
            words[j] = words[j].replace("\n","")
            kv = words[j].split("<>")
            k = kv[0]
            v = kv[1:]
            st[uni(k)] = []
            for val in v:
                st[uni(k)].append(uni(val))
        #print words
        posts.insert(st)   
        #print linereturn
    #print post


def group(st):
    if st == "-":
        return {}
    hm = {}
    parts = st.split("<>")
    for part in parts:
        kv = part.split(":")
        k = kv[0]
        v = kv[1:]
        hm[k] = []
        for val in v:
            hm[k].append(val)
    return hm

def groupall(st):
    if st == "-":
        return {}
    hm = {}
    parts = st.split(";")
    for part in parts:
        kv = part.split(":")
        k = kv[0]
        v = kv[1:]
        hm[k] = {"$all":[]}
        for val in v:
            hm[k]["$all"].append(val)
    return hm

def make_good(st):
    i = st.find("(")
    if i > -1:
        return st[:i].strip()
    else:
        return st

#find_new("Lagaan","type:film;category:indian film","starring","-","type:Actor:politition",5)
def find_new(coll,n1,t1,rel,n2,t2):   
    parts1 = groupall(t1)
    #print parts1
    parts2 = groupall(t2)
    #print parts2
    if n1 == "-":
        re1 = coll.find(parts1)
    else:
        parts1["key"] = {"$regex":re.compile(".*("+n1+").*")}
        re1 = coll.find(parts1)
        
    #print re1.count()
    for res in re1:
       #print res
       if res.has_key(rel):
           res_next = res[rel]
           for res1 in res_next:
               res1 = make_good(res1)
               #print "NOT",res1
               if n2 == "-":
                   parts2["key"] = {"$regex":re.compile(".*("+res1+").*")}
                   if coll.find(parts2).count():
                      yield res1
               else:
                   if res1 == n2:
                      parts2["key"] = {"$regex":re.compile(".*("+res1+").*")}
                      if coll.find_one(parts2).count():
                          yield res1
       

def find_relation(coll,n1,t1,n2,t2,N,visited):
    #print "====================",N,"===================="
    #print "n1",n1
    #print "t1",t1
    #print "n2",n2
    #print "t2",t2
    if N == 0:
         yield "-"
    kys = ["relative","spouse","starring","child","influencedby","friends","parent","narrater","director","editor","editing","writer","author"]
    if not n1 == "-":
        if n1 in visited:
            yield "-"
        else:
            visited.append(n1)
            parts1 = groupall(t1)
            #print parts1
            parts2 = groupall(t2)
            #print parts2
            if n1 == "-":
                docs1 = coll.find(parts1)
            else:
                parts1["key"] = {"$regex":re.compile(".*("+n1+").*")}
                docs1 = coll.find(parts1)
                
                
            for doc1 in docs1:
                #print doc1
                for key in kys:
                    #print "key",key
                    if doc1.has_key(key):
                        #print "has key"
                        for val in doc1[key]:
                            if not val == doc1["key"]:                        
                                #print "val",val
                                if n2 == "-":
                                    #print "n2==val"
                                    parts2["key"] = {"$regex":re.compile(".*("+val+").*")}
                                    #print parts2
                                    docs2 = coll.find(parts2)
                                    if not docs2 == None and docs2.count():
                                      for doc2 in docs2:
                                          yield doc1["key"]+" >>> "+key+" >>> "+doc2["key"]
                                    else:
                                      for rel in find_relation(coll,val,"-",n2,t2,N-1,visited):
                                          yield doc1["key"]+" >>> "+key+" >>> "+rel
                                
                                elif n2 == val:
                                    #print "n2==val"
                                    if not t2 == "-":
                                        parts2["key"] = {"$regex":re.compile(".*("+n2+").*")}
                                        #print parts2
                                        docs2 = coll.find_one(parts2)
                                        if not docs2 == None:
                                            yield doc1["key"]+" >>> "+key+" >>> "+val
                                        else:
                                            yield "-"
                                    else:
                                        yield doc1["key"]+" >>> "+key+" >>> "+val
                                else:
                                    for rel in find_relation(coll,val,"-",n2,t2,N-1,visited):
                                        yield doc1["key"]+" >>> "+key+" >>> "+rel
    yield "-"
                            
def find(txt,n1,n2,N):
    if N == 0:
        yield "-"
    for p in txt.find({"name":n1}):
        if n2 in p['friends']:
            yield n1+" friends with "+n2
        else:
            for k in p['friends']:
                ret = find(txt,k,n2,N-1)
                if ret == "-":
                    pass
                else:
                    for val in ret:
                        yield n1+" friends with "+val
    yield "-"
    


def query_relation(posts,n1,t1,n2,t2,N):
    hm = {}
    for i in find_relation(posts,n1,t1,n2,t2,N,[]):
        if i.find("-") > -1:
            pass
        else:
            if hm.has_key(i):
                pass
            else:
                hm[i] = 1
                yield i
    yield "DONE"
