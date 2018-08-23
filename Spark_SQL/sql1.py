from pyspark.sql import SQLContext
sq  = SQLContext(sc)
#df  = sq.read.json("file:/home/ks/hdpcd_spark/data/people.json")
df  = sq.read.json("file:/home/kamel_dev/data/people.json")

df.show()
+---+-------+
|age|   name|
+---+-------+
| 20|Michael|
| 30|   Andy|
| 19| Justin|
| 49|   Karl|
| 35|   John|
| 31|  Marin|
| 52|    Bob|
+---+-------+

# to use SQL, we need to register the dataframe df first
df.registerTempTable("myDF_tab")
df2  = sq.sql("select * from myDF_tab limit 3") # top 3
+---+-------+
|age|   name|
+---+-------+
| 20|Michael|
| 30|   Andy|
| 19| Justin|
+---+-------+
+---+-------+
|age|   name|
+---+-------+
| 20|Michael|
| 30|   Andy|
| 19| Justin|
+---+-------+



df.printSchema()

root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true
         
df.select("name").show(3)

+-------+
|   name|
+-------+
|Michael|
|   Andy|
| Justin|
+-------+

same as 

df.select(df["name"]).show(3)



df.filter(df["age"]>30).show()

+---+-----+
|age| name|
+---+-----+
| 49| Karl|
| 35| John|
| 31|Marin|
| 52|  Bob|
+---+-----+


df.groupBy("age").count().show()
+---+-----+
|age|count|
+---+-----+
| 31|    1|
| 35|    1|
| 49|    1|
| 52|    1|
| 19|    1|
| 20|    1|
| 30|    1|
+---+-----+



