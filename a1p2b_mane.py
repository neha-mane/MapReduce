import sys
import re
from pyspark import SparkContext, SparkConf
from bs4 import BeautifulSoup
import sys

conf = SparkConf().setAppName("Spark Count")
sc = SparkContext(conf=conf)

path = sys.argv[1]
#rdd = sc.wholeTextFiles(path)
rdd = sc.wholeTextFiles('/home/neha/StonyBrook/BDA/subset/*')
keys = rdd.map(lambda x: x[0])
values = rdd.map(lambda x:x[1])

#Splits keys that hold the path along with file name to extract industry name
splitkey = keys.map(lambda x:x.split(".")).\
	   	map(lambda y: y[3]).\
		map(lambda z:z.lower())

indus_set = splitkey.toLocalIterator()
indus_list = []

#Creates a broadcast variable of the set of industry values
for indus in indus_set:
	indus_list.append(indus)
broadcast_industry = sc.broadcast(list(indus_list))

list1 = []
#This function is to parse the date and post tags and extract date and occurances of industry name
def regex(value):
	result = []
	#print(value)
	value = value.replace("\n", "")
	value = value.lower()
	#print(value)
	value = value.replace("\r", "")
	Date = re.findall(r'<date>.*?</date>', value, re.M|re.I) #regular expression to get date
	#print(Date)
	for date in Date:
		date = date.replace("<date>", "")
		date = date.replace("</date>", "")
		date = date.split(",")
		dates = date[1]+"-"+date[2]
		#print(dates)
		Post = re.findall(r'<post>.*?</post>', value, re.M|re.I) #regular expression to get post
		for post in Post:
			post = post.split()
			for word in post:
				if word in indus_list:
					#print("inside", word)
					return(((word, dates), 1))
			
		return(((), 0))

#map dna filter the word dates
posts = values.map(lambda x:regex(x)).filter(lambda a:a[1] > 0).\
		reduceByKey(lambda x, y: x + y)

#convert into desired tuple and then reduceByKey
final = posts.map(lambda x: (x[0][0], [(x[0][1], x[1])]))
final1 = final.reduceByKey(lambda x,y: x+y)
finalres = final1.collect()
print(finalres)

