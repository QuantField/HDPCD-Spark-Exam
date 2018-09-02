# knn algorithm using spark
# finding the best number of nn for best classification
# the results are in knn2_res.txt
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster("local").setAppName("KNN")
sc   = SparkContext(conf = conf)

"""
from pyspark.sql import SQLContext, Row
sqC = SQLContext(sc)

df = sc.textFile("banana_train_1.csv")\
       .map(lambda line : line.split(","))\
       .map(lambda obs: Row(float(obs[0]),float(obs[1]),float(obs[2])))\
       .toDF(['x0','x1','y'])
"""

trainRDD = sc.textFile("banana_train_1.csv")\
           .map(lambda line : line.split(","))\
           .map(lambda line : [float(r) for r in line])

trainSet = trainRDD.collect()

trainVar = sc.broadcast(trainSet)

testRDD = sc.textFile("banana_train_3.csv")\
           .map(lambda line : line.split(","))\
           .map(lambda line : [float(r) for r in line])

def predict(x):
    # data point represent training point
    # s is a list of (distance from data point, label of data point)
    s =[ ((r[0]-x[0])**2 + (r[1]-x[1])**2, r[2]) for r in trainVar.value]
    # d represent the first nnn closest data point to x 
    d = sorted(s,key = lambda t : t[0])[:nnn.value]
    # as the class lable in this case are {-1,1} the voting proceeds as the sum
    # of the labels of the nnn closest points to x.
    res =  sum(r[1] for r in d)
    return 1.0 if res>=0 else -1.0

# getting the test data 


for r in [2,3,4,5,6,7,8,9,10]:
    nnn = sc.broadcast(r)
    dist = testRDD.map(lambda x : ( predict(x[0:2]) , x[2] ) )
    acc = dist.map(lambda x : x[0]==x[1]).reduce(lambda x,y: x+y)
    print "*----nn = %d ------ Prediction Accuracy = %2.2f ----------* " % (nnn.value,float(acc)/float(dist.count()))
