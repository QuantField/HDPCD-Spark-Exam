# HortonWroks HDPCD: Spark Exam

## The exam platform as of Aug 2018: 

- HDP 2.4.0
- Spark 1.6
- Scala 2.10.5
- Python 2.7.6 (pyspark)


## Exam Objectives:
------------------

## Core Spark
-------------

* Write a Spark Core application in Python or Scala

* Initialize a Spark application

```python
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
```
* Run a Spark job on YARN

* Create an RDD

* Create an RDD from a file or directory in HDFS

* Persist an RDD in memory or on disk

* Perform Spark transformations on an RDD

* Perform Spark actions on an RDD

* Create and use broadcast variables and accumulators

* Configure Spark properties

-------------
## Spark SQL
-------------

* Create Spark DataFrames from an existing RDD

* Perform operations on a DataFrame

* Write a Spark SQL application

* Use Hive with ORC from Spark SQL

* Write a Spark SQL application that reads and writes data from Hive tables


