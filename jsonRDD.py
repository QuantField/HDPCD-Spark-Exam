# convert a json to a data frame
from pyspark.sql import SQLContext
tqC = SQLContext(sc)

rdd = sc.parallelize([
  '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}'])

df = tqC.jsonRDD(rdd)
'''
>>> df.printSchema()
root
 |-- address: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- state: string (nullable = true)
 |-- name: string (nullable = true)
'''
