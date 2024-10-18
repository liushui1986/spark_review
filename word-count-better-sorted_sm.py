
# This code is a simple word count program using PySpark, a Python API for Apache Spark.
# re is Python's regular expression module, it is used to split text into words.
# SparkConf configures the settings for Spark, such as app name and execution mode
# SparkContext establishes a connection to a Spark cluster. It is entry point for working with Spark.
import re
from pyspark import SparkConf, SparkContext

# This function takes a string, converts it to lowercase and then splits it into words.
# The regular expression \W+ matches any sequence of non-word characters (anythong that isn't a letter, number, or underscore).
# It splits the text wherever a non-word character is found(e.g., punctuation, spaces).
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# SparkConf() creates a new configuration object for the Spark application.
# SparkContext is the main object in Spark used for creating RDDs, the fundamaental data structure in Spark.
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Loading the input text file
input = sc.textFile("file:///Users/mashu/Documents/study/spark/book.txt")
# FlatMapping the words: it applies the normalizeWords function to each line of text and falttens the result
# It breaks the lines into individual words.
words = input.flatMap(normalizeWords)

# Word count calculation
# map(lambda x: (x, 1)) maps each word to a tuple containing the word and the number 1, meaning each word is initialized with a count of 1.
# reduceByKey(lambda x,y: x + y): for each unique word, it adds up the counts (summing the 1s for each word).
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# Sorting the word counts
# map(lambda x: (x[1], x[0])): it swaps the order of the tuple from (word, count) to (count, word) to allow sorting by count.
# sortByKey(): it sorts the RDD by the key, here the key is the word count.
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
# collect() gathers the results back from the distributed RDD into the driver node (the local machine running the script).
results = wordCountsSorted.collect()

# The for loop interates through the results, which are tuples of (count, word).
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore') # Converts the word to ASCII encoding, ignoring non-ASCII characters.
    if (word):    # ensures the empty strings are not printed.
        print(word.decode() + ":\t\t" + count)   # Decodes the word back to a string and prints it alongside its count.
