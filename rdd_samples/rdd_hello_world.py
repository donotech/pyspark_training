import os

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame

os.environ['JAVA_HOME'] = "C:\\Program Files\\JetBrains\\IntelliJ IDEA Community Edition 2021.3.2\\jbr"
filepath = "..\\datasets\\dw_dataset\\sales_1.csv"
amount_field_pos = 3

spark = SparkSession.builder.appName("rdd_one").master("local[*]").getOrCreate()
rdd_lines: RDD = spark.sparkContext.textFile(filepath)
rdd_amounts: RDD = rdd_lines\
    .filter(lambda line: str(line.split(',')[amount_field_pos]) != 'total_amount') \
    .map(lambda line: (str(line.split(',')[amount_field_pos + 1]),
                       int(line.split(',')[amount_field_pos]))
         )

lines = rdd_amounts.collect()
for l in lines:
    print(l)

rdd_group = rdd_amounts.groupBy(lambda tup: tup[0])

lines = rdd_group.collect()
for l in lines:
    print(l)


# total_amount = rdd_amounts.sum()
# print(f'total_amount = {total_amount}')
