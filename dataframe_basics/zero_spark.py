import os
from pyspark.sql import SparkSession, DataFrame

os.environ['JAVA_HOME'] = "C:\\Program Files\\JetBrains\\IntelliJ IDEA Community Edition 2021.3.2\\jbr"

# github location for the file: https://github.com/donotech/pyspark_training/tree/main/datasets/dw_dataset

product_meta = "..\\datasets\\dw_dataset\\product_meta.csv"
spark: SparkSession = SparkSession.builder\
    .appName("rdd_one")\
    .master("local[*]")\
    .getOrCreate()

fp = open("..\\datasets\\dw_dataset\\product_meta.csv", "r")
with fp:
    lines = fp.readlines()
    for line in lines:
        print(line)

df: DataFrame = spark.read.csv(product_meta)
total_blr_records = df.agg({'sum', 'amount'}).collect()
print(total_blr_records)

df.show()
