import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_date
from pyspark.sql.types import StructType, IntegerType, StringType, StructField, DoubleType, DateType


def process_csv(spark: SparkSession):
      schema = StructType() \
          .add("RecordNumber", IntegerType(), True) \
          .add("Zipcode", IntegerType(), True)

      df_with_schema = spark.read.format("csv") \
          .option("header", True) \
          .schema(schema) \
          .load("/tmp/resources/zipcodes.csv")


def process_json(spark: SparkSession):
    # https://raw.githubusercontent.com/jokecamp/FootballData/master/World%20Cups/all-world-cup-players.json
    df = spark.read.option("multiline", "true").json("world-cup-players.json")
    df.show()


def process_json_with_schema(spark: SparkSession):

    # read schema json
    # for each field in schema json
    #     create a new structfield and add to json_schema
    #
    # schema evolution


    json_schema = StructType([
        StructField("name", StringType()),
        StructField("age", StringType()),
        StructField("gender", StringType())
    ])

    df = spark.read.option("header", "true").csv("C:\\Training\\pyspark_skai\\datasets\\dw_dataset\\customers.txt")
    df_demo = df.select("demographics")
    # df_demo.show()
    # df_demo.printSchema()
    df = df.withColumn("json_col", from_json(df["demographics"], json_schema))
    df.show()
    df.printSchema()
    # df.show()
    df.select("json_col.*").show()


os.environ['JAVA_HOME'] = "C:\\Program Files\\JetBrains\\IntelliJ IDEA Community Edition 2021.3.2\\jbr"
sales1 = "..\\dw_dataset\\sales_1.csv"
sales2 = "..\\dw_dataset\\sales_2.csv"
sales3 = "..\\dw_dataset\\sales_3.csv"
product_meta = "..\\dw_dataset\\product_meta.csv"
spark = SparkSession.builder.appName("rdd_one").master("local[*]").getOrCreate()

process_json_with_schema(spark)
# csv_schema = StructType([
#         StructField("temperatur", IntegerType()),
#         StructField("date_string", StringType()),
#         StructField("other_date", DateType())
#     ])
# df = spark.read.schema(csv_schema).csv("..\\datasets\\dw_dataset\\temperature_file.txt")
# fixed_date_df = df.withColumn("fixed_date", to_date(df["date_string"], "dd-MM-yyyy"))
# fixed_date_df.show()
# fixed_date_df.printSchema()
