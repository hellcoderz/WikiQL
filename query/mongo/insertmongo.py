import sys
import pymongo
from pymongo import Connection
import re
conn = Connection('localhost',int(sys.argv[2])
db = conn.mydb
posts = db.posts

def uni(st):
    return unicode(st,'utf-8', 'replace')

def insert():
    f = open(sys.argv[1],"r")
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