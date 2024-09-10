import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, current_timestamp, window, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DataType, DoubleType

os.environ['JAVA_HOME'] = "C:\\Program Files\\JetBrains\\IntelliJ IDEA Community Edition 2021.3.2\\jbr"
os.environ['HADOOP_HOME'] = "C:\\softwares\\hadoop3_winutils"
os.environ['hadoop.home.dir'] = "C:\\softwares\\hadoop3_winutils"

spark = SparkSession.builder.appName("spark_streaming")\
    .config("spark.driver.bindAddress", "localhost")\
    .config("spark.ui.port", "4051")\
    .master("local[*]").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 2)

news_dir = "C:\\tmp\\news\\"
price_dir = "C:\\tmp\\price\\"

news_schema = StructType([
    StructField("ticker", StringType()),
    StructField("news_text", StringType()),
    StructField("news_time", TimestampType())
])

news_df = spark.readStream.option("header", "true").csv(news_dir, news_schema)
news_df_with_ts = news_df.withColumn("news_arrival_time", current_timestamp())
news_df_with_wm = news_df_with_ts.withWatermark("news_arrival_time", "1 minutes")

# news_grouped_df = news_df_with_ts.groupBy(window(news_df_with_ts["news_arrival_time"], "30 second"),
#                                           news_df_with_ts["ticker"]).count()

# query = news_df_with_wm.writeStream.outputMode("update").format("console").start()

# news_df.printSchema()

price_schema = price_schema = StructType([
    StructField("ticker", StringType()),
    StructField("price", DoubleType()),
    StructField("price_time", TimestampType())])
price_df = spark.readStream.option("header", "true").csv(price_dir, price_schema)
price_df_with_ts = price_df.withColumn("price_arrival_time", current_timestamp())
price_df_with_wm = price_df_with_ts.withWatermark("price_arrival_time", "1 minutes")


# # price_df.printSchema()
# # query = price_df.writeStream.outputMode("append").format("console").start()
#
# joined_df = news_df.join(price_df, ["ticker"])
#
# query = joined_df.writeStream.outputMode("append").format("console").start()


joined_df = news_df_with_wm.join(price_df_with_wm,
                                 (news_df_with_wm["ticker"] == price_df_with_wm["ticker"]) &
                                 (news_df_with_wm["news_arrival_time"] <= price_df_with_wm["price_arrival_time"]))
#
query = joined_df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
query.stop()

