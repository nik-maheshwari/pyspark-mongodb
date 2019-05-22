from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# changed "local" and "nik" to your own db and collection names.
# creates spark context to read/write mongodb collections

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.0' ) \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/local.nik") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/local.analyses") \
    .getOrCreate()
my_spark2 = SQLContext(my_spark)

# read from default source, as defined above under 'input.uri'
data = my_spark2.read.format("com.mongodb.spark.sql.DefaultSource").load()

# look at top 5 rows to make sure data is read correctly
data.head(5)

# get summary of all columns
data.describe().show()

# Group the data by 'borough' and 'year', get the total count of crime 'value'.
# Store the returned dataframe in variable
crimeBoroughYear = data.groupBy([ "borough", "year"]).agg({'value': 'count'})

crimeBoroughYear.head(5)

# write the analysis result to local csv file. It creates a folder of the name
# 'analyses.csv' which contains the resulting csv file
crimeBoroughYear.repartition(1).write \
  .option("header", "true") \
  .csv("analyses.csv")

# Group the data by 'year', 'month' and crime 'category', and get the total
# count of crime 'value'.
crimeYearMonthCat = data.groupBy([ "year", "month", "major_category"]).agg({'value': 'count'})

# write the analysis result to local csv file. It creates a folder of the name
# 'analyses2.csv' which contains the resulting csv file
crimeYearMonthCat.head(5)
crimeYearMonthCat.write \
  .option("header", "true") \
  .csv("analyses2.csv")

# write the analysis result to mongodb, as defined under 'output.uri'
crimeBoroughYear.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

# stop the spark connection
my_spark.stop()