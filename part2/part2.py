from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# The entry point into all functionality in Spark is the SparkSession class.

input_file = sys.argv[1]
output_file = sys.argv[2]

spark = (SparkSession
	.builder
	.master("spark://10.10.1.1:7077")
    .appName("Simple Spark Application")
    .config("spark.some.config.option", "some-value")
    .getOrCreate())

# You can read the data from a file into DataFrames
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://10.10.1.1:9000" + input_file)
#df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("./export.csv")
df = df.orderBy(col('cca2').asc(),col('timestamp').asc())
df.show(10)
df.write.option("header", "true").csv("hdfs://10.10.1.1:9000" + output_file, mode = "overwrite")