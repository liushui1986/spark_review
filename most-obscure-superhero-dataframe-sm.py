from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# SuperSession.builder creates a Spark session
# .appName() sets the name of the Spark application, which is useful for monitoring the job.
spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

# Define the schema for the superhero names
schema = StructType([
                     StructField("id", IntegerType(), True), # The True means this field can contain null values.
                     StructField("name", StringType(), True) # The True means this field can contain null values.
                     ])

# It reads the data into a DataFrame using the previously defined schema.
# .option('spe', ' ') specifies that the separator in the CSV file is a space character (' ').
# the output DataFrame names has the superhero names and their IDs.
names = spark.read.schema(schema).option("sep", " ").csv("file:///Users/mashu/Documents/study/spark/Marvel-names.txt")

# Load superhero connections
# spark.read.text() reads the file as plain text. Each line of the file will be a single row in the output DataFrame
# The DataFrame is stored in a column called value.
lines = spark.read.text("file:///Users/mashu/Documents/study/spark/Marvel-graph.txt")

# Extract connections per superhero
# func.trim(func.col('value')) trims any leading and trailing whitespace from the value column
# func.split(..., ' ') splits the line into an array, using space as the delimiter.
# .withColumn('id', ...) adds a new column id containing the extracted ID.
# func.size(...) computes the size(number of elements) of this array.
# .agg(func.sum(...).alias(...)) aggregates the sum of connections for each superhero.
# If a superhero appears in multiple lines, this ensures the total number of connections in counted correctly.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Find the minimum connection count
# .agg(func.min('connections')) aggregates the minimum value of the connections column
# .first()[0] fetches the first(and only) row of the result, and extracts the minimum number of connections from the first column
minConnectionCount = connections.agg(func.min('connections')).first()[0]

# filter superheroes with the minimum connection count
mostObscureID = connections.filter(func.col("connections") == minConnectionCount)

# Join with names to get the name of the most obscure superhero.
# .join(names, 'id') performs an inner join between the mostObscureID DataFrame and the names DataFrame on the id column
mostObscureIDwithName = mostObscureID.join(names, 'id').select('name')

# Show the result
mostObscureIDwithName.show()

spark.stop()
