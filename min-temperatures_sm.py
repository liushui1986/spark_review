# This PySpark script processes a CSV file containing temperature data from various weather stations.

# Import PySpark libraries
# SparkConf is used to configure the Spark application
# SparkContext represents the connnection to the Spark cluster and is used to initialize RDDs
from pyspark import SparkConf, SparkContext

# Configure Spark
# setMaster("local") means running Spark job locally (on one machine)
# setAppName means naming the application for identification in logs or UI
# SparkContext creates a Spark context, which is the entry point to using Spark's features.
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    """
    the function takes a line from the CSV file, splits it by commas, and extracts stationID,
    entryType and temperature
    Parameters
    ----------
    line : str
        a string from the csv file, splitted by commas.

    Returns
    -------
    stationID : str
        station ID.
    entryType : str
        min or max or others.
    temperature : str
        temperature.

    """
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    
    # fields[3] contains number in tenths of degrees Celsius.
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0  
    return (stationID, entryType, temperature)

# Load data and parse lines
# Loads the CSV file into an RDD, with each line as a string
lines = sc.textFile("file:///Users/mashu/Documents/study/spark/1800.csv")

# Applies the parseline function to each line, creating a new RDD where each record is a tuple
parsedLines = lines.map(parseLine)

# Filter for minimum temperatures
# Filters the RDD to keep only records where the entryType is "TMIN".
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

# MAP to (stationID, temperature) pairs
# Transforms the RDD to only include the station ID and temperature, ignoring the entryType (all "TMIN")
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

# Find the minimum temperature per Station
# Aggregates the temperatures by station ID, keeping only the minimum temperature for each station.
# It compares two temperature values x and y and returns the lower one.
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

# Collect the results
# Brings the results from all Spark workers to the driver node(local machine), returning a list of pairs
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
