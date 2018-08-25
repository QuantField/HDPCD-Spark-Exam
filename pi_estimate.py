#--------------------------------------------------------------------------------------
#--- PySpark 1.6 application that uses Monte-Carlo method to estimate PI
#---------------------------------------------------------------------------------------
from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("PI Estimate")
sc   = SparkContext(conf = conf)

import sys
import random

def inside_circle(_):
    x,y = random.random(), random.random()
    return 1.0 if x**2+y**2<=1.0 else 0.0

# usage runnig this application:
# spark-submit  pi_spark.py 10
# this means number of partitions = 10

partitions = int(sys.argv[1]) if len(sys.argv)>1 else 2
# defautl number of partitions is 2

N   = 100000*partitions
cnt = sc.parallelize(list(range(N)),partitions).map(inside_circle).reduce(lambda x,y:x+y)
res = cnt*4.0/N
print "Pi = %f" %res
