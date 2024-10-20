# -*- coding: utf-8 -*-
"""
Created on Mon Sep  7 15:28:00 2020
Modified on Sat Oct 19 21:41:00 2024

@author: Frank
@author: Shuaipeng
"""

# This PySpark code enhances the previous movie popularity analysis by adding movie titles to the results.
# It achieves this by loading movie names from a file (u.ITEM) and creating a lookup function using a broadcasted dictionary.
from pyspark.sql import SparkSession   # Entry point to use Spark DataFrame API
from pyspark.sql import functions as func   # provides built-in PySpark functions for transformations
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs   # used to open the u.ITEM file, which contains movie names, in the correct encoding (ISO-8859-1).

def loadMovieNames():
    """
    Loading movie names into a discionary.
    It reads the u.ITEM file (which contains the movie IDs and titles) into a dictionalry movieNames.
    
    Returns
    -------
    movieNames : TYPE
        DESCRIPTION.

    """
    movieNames = {}
    with codecs.open("C:/Users/mashu/Documents/study/spark/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')  # the file format uses the pip character (|) as a delimiter.
            movieNames[int(fields[0])] = fields[1]  # field[0]: Movie ID. field[1]: Movie title.
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# broadcast() method creates a shared, read-only variable across all Spark executors.
# It makes the movie name dictionary available on all nodes for efficient lookups.
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie ratings data as dataframe
# The u.data file contians the movie rating data, and it is loaded into a DataFrame. It is tab-separated(\t).
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///Users/mashu/Documents/study/spark/ml-100k/u.data")

movieCounts = moviesDF.groupBy("movieID").count()

# Create a user-defined function (UDF) to look up movie names from our broadcasted dictionary
def lookupName(movieID):
    """
    It takes a movieID and retrieves the corresponding movie title from the broadcasted dictionary nameDict.value

    Parameters
    ----------
    movieID : int
        The ID of them movie.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    return nameDict.value[movieID]

# It registers the function as a PySpark UDF, allowing it to be used within DataFrame transformations.
lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column to the movieCounts DataFrame by applying the lookupNameUDF to the movieID column.
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Grab the top 10 without truncating the movie titles.
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()
