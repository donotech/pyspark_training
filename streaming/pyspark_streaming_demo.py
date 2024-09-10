import os
import time

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col, lit, when, rank, row_number, broadcast
# df2 = df.select(col("EmpId"),col("Salary"),lit("1").alias("lit_value1"))
# df2.show(truncate=False)

from pyspark.sql.functions import when

os.environ['JAVA_HOME'] = "C:\\Program Files\\JetBrains\\IntelliJ IDEA Community Edition 2021.3.2\\jbr"
os.environ['HADOOP_HOME'] = "C:\\softwares\\hadoop3_winutils"
os.environ['hadoop.home.dir'] = "C:\\softwares\\hadoop3_winutils"

spark = SparkSession.builder.appName("spark_streaming")\
    .config("spark.driver.bindAddress", "localhost")\
    .config("spark.ui.port", "4051")\
    .master("local[*]").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 2)

read_folder = "C:\\tmp\\pyspark_stream\\"
word_type_file = "C:\\tmp\\word_types.csv"
# lines = spark.readStream.text("file:///mnt/home/jgcslabtrainer2001/some_files")
# to read from local filesystem: run the shell as pyspark2 --master local[2]
# assume
word_type_df = spark.read.option("header", "true").option("inferSchema", "true").csv(word_type_file)
lines = spark.readStream.text(read_folder)
words = lines.select(explode(split(lines.value, " ")).alias('word'))
word_type_joined = words.join(word_type_df, ['word'], "left_outer")
# wordCounts = words.groupBy("word").count()
# query = wordCounts.writeStream.outputMode("update").format("console").start()
query = word_type_joined.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
query.stop()

# word_class_df = spark.read.option("header", "true").csv("word_class/file_article.txt")
# word_class_df.show()
#
#
# joined_df = words.join(word_class_df, ["word"])
# wordClassCounts = joined_df.groupBy("word").count()
# writer2 = wordClassCounts.writeStream.outputMode("complete").format("console")
# query2 = writer2.start()
#
