from pyspark.sql import SparkSession
from pyspark.sql import functions as func
# StructType and StructField are used to define a schema for the DataFrame, allowing you to specify the structure of your data.
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("Totalspend").getOrCreate()

# Define the schema
schema = StructType([ \
                     StructField("custID", IntegerType(), True), \
                     StructField("transID", IntegerType(), True), \
                     StructField("amount", FloatType(), True)
                     ])

# read the csv file into a dataframe
df = spark.read.schema(schema).csv("file:///Users/mashu/Documents/study/spark/customer-orders.csv")
# printSchema() method displays the structure of the DataFrame, showing the column names and data types.

# The select method is used to create a new DataFrame custAmount that includes only custID and amount
custAmount = df.select("custID", "amount")

# Aggregate to find sum of spending
totalSpend = custAmount.groupBy("custID").sum("amount")
totalSpend.show()

totalSpends = totalSpend.withColumn("total_spent",
                                                  func.round(func.col('sum(amount)'), 2))\
                                                  .select("custID", "total_spent").sort("total_spent")
                                                  
totalSpends.show(totalSpends.count())
    
spark.stop()

                                                  