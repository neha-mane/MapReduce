import sys
from random import random
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Spark Count")
sc = SparkContext(conf=conf)

#-------------Word Count Implementation Below-----------
values = sc.textFile("input.txt")
counts = values.map(lambda line: line.split(" ")).\
	     map(lambda z: z.lower()).\
             map(lambda word: (word, 1)).\
		reduceByKey(lambda a,b: a+b)
output = counts.collect()
print (repr(output)[1:-1])
sc.stop()

#------------Set Difference Implementation Below---------

conf1 = SparkConf().setAppName("Spark Set Difference")
sc1 = SparkContext(conf=conf1)

data = [('R', [x for x in range(50) if random() > 0.5]),
           ('S', [x for x in range(50) if random() > 0.75])]

rdd = sc1.parallelize(data)
final = rdd.map(lambda z: (y, 1) for y in z[1] if z[0] == 'R' and z[0] != 'S').\
	filter(x[1]== 1)

result = final.collect()
print(result)





