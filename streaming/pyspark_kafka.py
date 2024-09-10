import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, split, explode, current_timestamp, window, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DataType, DoubleType

os.environ['JAVA_HOME'] = "C:\\Program Files\\JetBrains\\IntelliJ IDEA Community Edition 2021.3.2\\jbr"
os.environ['HADOOP_HOME'] = "C:\\softwares\\hadoop3_winutils"
os.environ['hadoop.home.dir'] = "C:\\softwares\\hadoop3_winutils"

packages = ['org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2']

spark = SparkSession.builder.appName("spark_streaming")\
    .config("spark.jars.packages", ",".join(packages))\
    .master("local[*]").getOrCreate()
    # .config("spark.driver.bindAddress", "localhost")\
    # .config("spark.ui.port", "4051")\


spark.conf.set("spark.sql.shuffle.partitions", 2)

raw_df = spark.readStream.format("kafka")\
      .option("kafka.bootstrap.servers", "localhost:9092")\
      .option("subscribe", "socgenl2python")\
      .load()

news_schema = StructType([
    StructField("ticker", StringType()),
    StructField("news", StringType()),
    StructField("date_of_news", TimestampType())
])
casted_df = raw_df.withColumn("value_string", raw_df["value"].cast("string"))
json_df = casted_df.withColumn("json_value", from_json(casted_df["value_string"], news_schema))
main_df = json_df.select("json_value.*", "timestamp", "offset", "partition", "topic")
query = main_df.writeStream.outputMode("append").format("kafka").start()

query.awaitTermination()
query.stop()
