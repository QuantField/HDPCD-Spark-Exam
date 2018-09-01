#
# very important !!!
# in order for sql to work it is important to declare the first two lines below before we start using the the sc default context, sc.parallilize for example. If we work with sc and then create SQLContext thihg can get weird .

from pyspark.sql import SQLContext
sqC    = SQLContext(sc)

data = [(1, "John", 25), (2, "Ray", 35), (3,"Mike", 24), (4, "Jane", 28), 
(5, "Kevin", 26), (6, "Vincent", 35), (7,"James", 38), (8, "Shane", 32), 
(9, "Larry", 29), (10, "Kimberly", 29),(11, "Alex", 28), (12, "Garry", 25), 
(13, "Max",31)]

employees = sc.parallelize(data).toDF(["emp_id","name","age"])

"""
st = employees.sort('age', ascending=False).show()
+------+--------+---+
|emp_id|    name|age|
+------+--------+---+
|     7|   James| 38|
|     2|     Ray| 35|
|     6| Vincent| 35|
|     8|   Shane| 32|
|    13|     Max| 31|
|     9|   Larry| 29|
|    10|Kimberly| 29|
|    11|    Alex| 28|
|     4|    Jane| 28|
|     5|   Kevin| 26|
|     1|    John| 25|
|    12|   Garry| 25|
|     3|    Mike| 24|
+------+--------+---+
the same thing can be achieved using 
employees.orderBy('age',ascending=False).show()
"""
# parquet is an efficient columnar format  
# saving as a parquet file.. put in my hdfs user folder,i.e. under /user/kamel_dev/
# employees.write.parquet("employee.parquet")
# for reading ... sqC.read.parquet("employee.parquet")

salary = sqC.read.json("file:/home/kamel_dev/data/salary.json")
designation = sqC.read.json("file:/home/kamel_dev/data/designation.json")

# Consolidating data 
# salary.columns this is to list column names
# ['e_id', 'salary']
# designation.columns                                                      
# ['id', 'role']

final_data = employees.join(salary, employees.emp_id==salary.e_id)\
            .join(designation, employees.emp_id==designation.id )\
            .select('emp_id','name','role','salary')
final_data.show()
+------+--------+--------------+------+
|emp_id|    name|          role|salary|
+------+--------+--------------+------+
|     1|    John|     Associate| 10000|
|     2|     Ray|       Manager| 12000|
|     3|    Mike|       Manager| 12000|
|     4|    Jane|     Associate|  null|
|     5|   Kevin|       Manager|   120|
|     6| Vincent|Senior Manager| 22000|
|     7|   James|Senior Manager| 20000|
|     8|   Shane|       Manager| 12000|
|     9|   Larry|       Manager| 10000|
|    10|Kimberly|     Associate|  8000|
|    11|    Alex|       Manager| 12000|
|    12|   Garry|       Manager| 12000|
|    13|     Max|       Manager|120000|
+------+--------+--------------+------+

#---- Let's do this in SQL

employees.registerTempTable("emp")
salary.registerTempTable("sal")
designation.registerTempTable("des") 




# dropping missing values 
