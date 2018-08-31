# load a csv file
from pyspark.sql import SQLContext, Row
sq  = SQLContext(sc)
rdd  = sc.textFile("file:/home/kamel_dev/data/people.txt")\
            .map(lambda x : x.split(","))\
            .map(lambda x : Row(name = x[0], age=int(x[1])))
# Infer the schema, and convert to data frame 
schemaPeople = sq.createDataFrame(rdd)

#register the DataFrame as a sql table
schemaPeople.registerTempTable("people")

young = sq.sql("SELECT * FROM people WHERE age >= 13 AND age <= 30")
young.show()
+---+---------+
|age|     name|
+---+---------+
| 20|"Michael"|
| 30|   "Andy"|
| 19| "Justin"|
+---+---------+

