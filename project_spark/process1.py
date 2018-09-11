# the data can be downloaded from 
# https://drive.google.com/open?id=1Hid-RrefaSjgQGGNDcvhD-R-rIv9GX0u
#
from pyspark.sql import SQLContext,Row
sqC = SQLContext(sc)

rdd      =sc.textFile("/home/kamel_dev/spData/tv_view.csv")
firstRow = rdd.first()

def del_double_quotes(x):
    return [ r.replace('"','') for r in x ]

# deleting " from variable names in the header
header   = del_double_quotes( firstRow.split(","))

# getting the data without the header
rddData = rdd.filter(lambda line : line != firstRow)


df = rddData.map(lambda line : line.split(","))\
            .map(  del_double_quotes)\
            .map(lambda line : Row(line[0],line[1],line[2],line[3],line[4],line[5],
                line[6],line[7],line[8],line[9],line[10],
                line[11],line[12],line[13],line[14],line[15])).toDF(header)

number_of_rows = df.count() #3863142            

housid = df.groupBy('household_id').count()            

df_train = df[df['dob']!='1900-01-01']
df_test  = df[df['dob']=='1900-01-01']

n_tr = df_train.count()
n_te = df_test.count()

ds = df.select('household_id').distinct()
>>> ids.head()
Row(household_id=u'432216801')                                                  
>>> ids.count()
2813                                                                            
>>> 
>>> df.registerTempTable("myTab")
>>> sqC.sql("select count(*) as cnt from myTab")
DataFrame[cnt: bigint]
>>> sqC.sql("select count(*) as cnt from myTab").show()
+-------+                                                                       
|    cnt|
+-------+
|3863142|
+-------+







