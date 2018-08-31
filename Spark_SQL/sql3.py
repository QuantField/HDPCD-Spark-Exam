from pyspark.sql import SQLContext, Row
sqlC = SQLContext(sc)
# if we want to import json files it will be with SQLContext above
# not with the default sc
# uploading csv file

rdd = sc.textFile("/user/kamel_dev/housing.csv")
header = rdd.first()
rdd2 = rdd.filter(lambda line: line!=header)

# To create a dataframe, tow ways :

# ---1-----
df = rdd2.map(lambda line : line.split(","))\
        .map(lambda line : Row( longitude=float(line[0]),
                                latitude=float(line[1]),
                                housing_median_age=float(line[2]),
                                total_rooms=float(line[3]),
                                total_bedrooms=float(line[4]),
                                population=float(line[5]),
                                households=float(line[6]),
                                median_income=float(line[7]),
                                median_house_value=float(line[8]),
                                ocean_proximity=line[9])).toDF()
# -----1b----
# let's assume rddX is the the above statement but without the .toDF() at the end
df = sqlC.createDataFrame(rddX) # this had the same effect as 1   
# -----2-----
cols = header.split(",")
df2 = rdd2.map(lambda line : line.split(","))\
        .map(lambda line : Row( float(line[0]),
                                float(line[1]),
                                float(line[2]),
                                float(line[3]),
                                float(line[4]),
                                float(line[5]),
                                float(line[6]),
                                float(line[7]),
                                float(line[8]),
                                line[9])).toDF(cols)
        
df.printSchema()

root
 |-- longitude: double (nullable = true)
 |-- latitude: double (nullable = true)
 |-- housing_median_age: double (nullable = true)
 |-- total_rooms: double (nullable = true)
 |-- total_bedrooms: double (nullable = true)
 |-- population: double (nullable = true)
 |-- households: double (nullable = true)
 |-- median_income: double (nullable = true)
 |-- median_house_value: double (nullable = true)
 |-- ocean_proximity: string (nullable = true)


#------ to use sql queries we must register the table first

df.registerTempTable("housing")

sqlC.sql("select  ocean_proximity  from housing limit 3").show()
+---------------+
|ocean_proximity|
+---------------+
|       NEAR BAY|
|       NEAR BAY|
|       NEAR BAY|
+---------------+

df.select(df['ocean_proximity']).show(3) # same as sql statement above
