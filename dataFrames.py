'''
Rank the users by number of daily hobbies. That is:

    Filter and keep only the daily hobbies.
    Join users to (daily) hobbies
    Aggregate and count the daily hobbies per users
    Rank the resulting table by number of daily hobbies
'''

from pyspark.sql import SQLContext
sqC = SQLContext(sc)

users = sc.parallelize([(0, "Hans"),(1, "Peter")]).toDF(['ID','name'])

hobbies = sc.parallelize([
(0, "gym", "daily"),
(0, "drawing", "weekly"),
(1, "reading", "daily"),
(1, "guitar", "weekly"),
(1, "gaming", "daily"),
(1, "movies", "daily")]).toDF(['userID', 'hobby', 'frequency'])

data1 = hobbies.filter(hobbies.frequency=='daily')\
               .join(users, users.ID == hobbies.userID)\
               .groupBy('name').count()
+-----+-----+
| name|count|
+-----+-----+
|Peter|    3|
| Hans|    1|
+-----+-----+


# or
data2 = hobbies.filter(hobbies.frequency=='daily')\
               .join(users, users.ID == hobbies.userID)\
               .groupBy('name')\
               .agg({'hobby':'count'})
+-----+------------+
| name|count(hobby)|
+-----+------------+
|Peter|           3|
| Hans|           1|
+-----+------------+

# rename column with selectExpr
data2b = data2.selectExpr('name as Name')

