from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# Initialize Spark session
spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

# Load dateset
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///Users/mashu/Documents/study/spark/fakefriends-header.csv")
    
# Select only 'age' and 'friends' columns
age_friends = people.select('age', 'friends')

# Group by 'age' and compute average number of friends
age_friends.groupBy('age').avg('friends').show()

# Sorted by 'age'
age_friends.groupBy('age').avg('friends').sort('age').show()

# Formatted with rounded average
age_friends.groupBy('age').agg(func.round(func.avg('friends'),2)).sort('age').show()

# Custom column name for averge and sorted by 'age'
age_friends.groupBy('age').agg(func.round(func.avg('friends'), 2).alias('friend_avg')).sort('age').show()

# Stop the Spark session
spark.stop()

