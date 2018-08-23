data = [('A',3),('B',4),('B',6),('C',12),('A',6),('A',3),('C',2),('B',7)]
rdd  = sc.parallelize(data)


rdd.combineByKey(createCombiner,
                 mergeValue,
                 mergeCombiner)

# For instance we try to calculate the average per key
"""
Create a Combiner

lambda value: (value, 1)

The first required argument in the combineByKey method is a function to be used as the very first aggregation step for each key. The argument of this function corresponds to the value in a key-value pair. If we want to compute the sum and count using combineByKey, then we can create this “combiner” to be a tuple in the form of (sum, count). The very first step in this aggregation is then (value, 1), where value is the first RDD value that combineByKey comes across and 1 initializes the count.
Merge a Value

lambda x, value: (x[0] + value, x[1] + 1)

The next required function tells combineByKey what to do when a combiner is given a new value. The arguments to this function are a combiner and a new value. The structure of the combiner is defined above as a tuple in the form of (sum, count) so we merge the new value by adding it to the first element of the tuple while incrementing 1 to the second element of the tuple.
Merge two Combiners

lambda x, y: (x[0] + y[0], x[1] + y[1])

The final required function tells combineByKey how to merge two combiners. In this example with tuples as combiners in the form of (sum, count), all we need to do is add the first and last elements together.
"""
It is a combine by Key, so we have to provide how to combine the values, we will need a sum and a count, and then later calculate the average.

1 - ('A',3) value is 3, the first step maps to (3,1) which is the combiner
    3 --> (3,1)
    lambda value : (value,1)

2 - merge value, combines a value to the combiner (3,2) and 6 for example
    this will yiels (3+6,2+1)
    (3,2) and 6 --> (3+6,2+1)
    lambda x, value  : (x[0]+value, x[1]+1),

3 - merge combinere ('A',(3,1)),('A',(4,2)) --> (3+4,1+2) 
    
comb = rdd.combineByKey(lambda value : (value,1),
                        lambda x, value  : (x[0]+value, x[1]+1),
                        lambda x, y      : (x[0]+y[0], x[1]+y[1]))

# result
[('A', (12, 3)), ('C', (14, 2)), ('B', (17, 3))]

rdd2 = comb.map(lambda x : (x[0],float(x[1][0])/float(x[1][1])))
# result
[('A', 4.0), ('C', 7.0), ('B', 5.666666666666667)]
