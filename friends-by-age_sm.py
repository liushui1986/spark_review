from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///Users/mashu/Documents/study/spark/fakefriends.csv")
rdd = lines.map(parseLine)

# Aggregate total number of friends and counts by age
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Calculate average number of friends by age
averagesByAge = totalsByAge.mapValues(lambda x: int(x[0] / x[1]))

# Sort by age
sortedResults = averagesByAge.sortByKey()

# Collect and print the results
results = sortedResults.collect()
for result in results:
    print(result)
