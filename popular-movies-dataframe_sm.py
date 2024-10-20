# This PySpark code snippet processes movie rating data to find the most popular movies based on the number of ratings they received.

from pyspark.sql import SparkSession
from pyspark.sql import functions as func   # it contains built-in functions for data transformations
from pyspark.sql.types import StructType, StructField, IntegerType, LongType  # define the schema for the dataset

# Initializes a Spark session named 'PopularMovies', which will allow you to interact with Spark for data processing
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
# The .read.option('sep', '\t') specifies that he file is tab-separated (\t).
# The schema(schema) ensures the file is read using the specified structure.
# The .csv() method loads the dataset from the given path into a DataFrame named movieDF.
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///Users/mashu/Documents/study/spark/ml-100k/u.data")

# Some SQL-style magic to sort all movies by popularity in one line!
# count() counts the number of ratings (rows) for each movie
# orderBy(func.desc('count')) sorts the movies in descending order of the count, showing the most rated(popular) movies at the top
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 10
topMovieIDs.show(10)

# Stop the session
spark.stop()
