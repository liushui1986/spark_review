from pyspark import SparkConf, SparkContext

# Create Spark configuration and context
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Load the text file
input = sc.textFile("file:///Users/mashu/Documents/study/spark/book.txt")

# Split each line into words by empty space
words = input.flatMap(lambda x: x.split())

# Map each word to a (word, 1) pair
wordPairs = words.map(lambda word: (word.encode('ascii', 'ignore').decode('ascii'), 1))

# Reduce to get the count of each word
wordCounts = wordPairs.reduceByKey(lambda x, y: x + y)

# Collect and print results
results = wordCounts.collect()

# Print word counts
for word, count in results:
    if word:  # Only print non-empty words
        print(word, count)
