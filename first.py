

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd_one").master("local[*]").getOrCreate()
print(spark)
