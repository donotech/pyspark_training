import pandas


df = pandas.read_csv("C:\\Training\\pyspark_skai\\datasets\\airlines.csv")

print(df['Airport.Code'].unique())