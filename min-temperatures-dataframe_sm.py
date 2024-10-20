from pyspark.sql import SparkSession
from pyspark.sql import functions as func
# StructType and StructField are used to define a schema for the DataFrame, allowing you to specify the structure of your data.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

# Define the schema
schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

# read the csv file into a dataframe
df = spark.read.schema(schema).csv("file:///Users/mashu/Documents/study/spark/1800.csv")
# printSchema() method displays the structure of the DataFrame, showing the column names and data types.
df.printSchema()

# Filter the DataFrame df to keep only the rows where measure_type is "TMIN". A new DataFrame callsed minTemps
minTemps = df.filter(df.measure_type == "TMIN")

# The select method is used to create a new DataFrame stationTemps that includes only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

# Convert temperature to fahrenheit and sort the dataset
# withColumn method creates a new column 'temperature' by converting the minimum temperature from C to F
# func.round rounds the F temperature to two decimal places.
# finally, only the stationID and the new temperature columns are selected, sorted by temperature.
minTempsByStationF = minTempsByStation.withColumn("temperature",
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                  .select("stationID", "temperature").sort("temperature")
                                                  
# Collect, format, and print the results
# The collect method retrieves all rows of the DataFrame minTempsByStationF as a list of Row objects, stored in results
results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
    
spark.stop()

                                                  