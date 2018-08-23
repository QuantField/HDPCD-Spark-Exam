# The purpose of this exercise is to calculate word frequency and list the top 10
# most frequent words in the provided text file.

rdd  = sc.textFile("lorum.txt") # this file is already copied to my hdfs directory

def clean(line):
    s = line.replace('.',' ').replace(',',' ').replace('?',' ')
    return s.lower()

rdd2 = rdd.map(clean).flatMap(lambda x : x.split())
rdd3 = rdd2.map(lambda x:(x,1))  # create Pair RDD
#--- count the word frequency 

#1-- using countByKey
count1 = rdd3.countByKey()

#2-- using reduceByKey
# collectAsMap() convert the RDD to a dictionary
count2 = rdd3.reduceByKey(lambda x,y : x+y).collectAsMap()

#3-- using combineByKey .. see the combineByKey.py for more info
count3 = rdd3.combineByKey(lambda value : value,
        lambda x,value : x+value,
        lambda x, y    : x + y).collectAsMap()

# words with highest frequency
s = sorted(count1.iteritems(), key = lambda (k,v):(v,k), reverse=True)
s[:10] # to 10 most frequent words
