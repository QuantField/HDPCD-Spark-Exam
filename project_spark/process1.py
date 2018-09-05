from pyspark.sql import SQLContext,Row
sqC = SQLContext(sc)

rdd      = sc.textFile("tv_view.csv")
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


housid = df.groupBy('household_id').count()            
