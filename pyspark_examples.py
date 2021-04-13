# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 10:38:42 2020

@author: Praveen.Kumar
"""

# to create a spark context
from pyspark import SparkContext
sc = SparkContext(appName="Python_examples")
print(sc)
sc

# spark web UI   : localhost:4040
# if u r running in the terminal no need to create a spark context. 

#Creating an rdd from using parallelize method.
data = "Hello"
rdd = sc.parallelize(data)
print(rdd)
#Action
rdd.collect() 

num_rdd = sc.parallelize(range(0,100))
num_rdd.take(10)
num_rdd.reduce(lambda x,y: x+y)


#testing apache spark on databricks
#testing apache spark on spyder console
#testing apache spark on jupiter notebook
#testing apache spark version 2.4
# rdd from a file
data = sc.textFile("C:/Users/Praveen.kumar/Desktop/sp_data.txt")   # windows. 
#data = sc.textFile("hdfs://IP:PORT/LOCATION")
data.take(2)
rdd1 = data.map(lambda x: x.upper(), data.values)
data.collect()

rdd = sc.parallelize(["Where is Mount Everest","In Himalayas India"])
map_rdd = rdd.map(lambda x: x.split(" "))
rdd.collect() 
map_rdd.collect()
rdd.count()

# map transformation
map_rdd = rdd.map(lambda x: x.split(" "))
map_rdd.collect()

#flat map transformation
flatmap_rdd = rdd.flatMap(lambda x: x.split(" "))
flatmap_rdd.collect()

# filter transformation
filtered_rdd = rdd.filter(lambda x: "Everest" in x)
filtered_rdd.collect()
filtered_rdd.count()      #1

##mapPartitions
#Typically you want 2-4 partitions for each CPU core in your cluster. 
#Normally, Spark tries to set the number of 
#partitions automatically based on your cluster or hardware based on standalone environment.
# 1,2,3,4,5,6,7,8,9
one_through_9 = range(1,10)

#1,2,3 -- 4,5,6 -- 7,8,9
parallel = sc.parallelize(one_through_9, 3) # 3 - no of partitions. 
def f(iterator): yield sum(iterator)
parallel.mapPartitions(f).collect()

# 1,2,3,4,5 -- 6,7,8,9
parallel = sc.parallelize(one_through_9, 2)
def f(iterator): yield sum(iterator)
parallel.mapPartitions(f).collect()

#1,2 --3,4 --5,6 -- 7,8,9
parallel = sc.parallelize(one_through_9)
parallel.mapPartitions(f).collect()

# mapPartitions with index
parallel = sc.parallelize(range(1,10),4)
def show(index, iterator): yield 'index: '+str(index)+" values: "+ str(list(iterator))
parallel.mapPartitionsWithIndex(show).collect()

parallel = sc.parallelize(range(1,10),3)
def show(index, iterator): yield 'index: '+str(index)+" values: "+ str(list(iterator))
parallel.mapPartitionsWithIndex(show).collect()


# union of two RDD
one = sc.parallelize(range(1,10))
one.persist()
two = sc.parallelize(range(10,21))
one.union(two).collect()

one.persist()

one.unpersist()


# intersection of two rdd
one = sc.parallelize(range(1,10))
two = sc.parallelize(range(5,15))
one.intersection(two).collect()

# distinct 
parallel = sc.parallelize(range(1,9))
par2 = sc.parallelize(range(5,15))
parallel.union(par2).distinct().collect()



# to stop a spark context
sc.stop()
















# creating a dataframe using python
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark import SparkContext

sc = SparkContext(appName="Python_examples")
sqlContext = SQLContext(sc)

sqlContext

l = [('Ankit',25),('Jalfaizy',22),('saurabh',20),('Bala',26)]

rdd = sc.parallelize(l)

people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
people

schemaPeople = sqlContext.createDataFrame(people)

type(schemaPeople)

schemaPeople.printSchema()

schemaPeople.select("name")
schemaPeople.select("name","age").show()


schemaPeople.take(3)

schemaPeople.count()

schemaPeople.describe().show()


rdd = sc.textFile("C:/Users/Praveen.kumar/Desktop/textdocz/customer.txt")
rdd.take(5)

parts = rdd.map(lambda l: l.split(","))

customer = parts.map(lambda p:Row(id=p[0],name=p[1],city=p[2]))

schemaCustomer=sqlContext.createDataFrame(customer)
type(schemaCustomer)

schemaCustomer.registerTempTable("customer")

result=sqlContext.sql("select id from customer")
result
result.show()

result = sqlContext.sql("select * from customer")
result.show()

sc.stop()




































