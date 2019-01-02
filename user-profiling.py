import csv
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add

# SPARK SESSION
ss = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath", "/home/ciori/Projects/big-data-project/mariadb-java-client-2.3.0.jar") \
    .master("local[*]") \
    .appName("Big Data Project Python - SQL") \
    .getOrCreate()

# SPARK CONTEXT
sc = ss.sparkContext

'''# flatmap function
def init_counts(line):
    new_lines = []
    user_id = str(line[0])
    text = str(line[1])
    for keyword in text.split(" "):
        #new_line = [user_id + "_" + keyword, 1]
        new_line = [user_id, keyword, 1]
        new_lines.append(new_line)
    return new_lines

# read tweet_keywords table
tweet_keywords_DF = ss.read \
    .format("jdbc") \
    .option("url", "jdbc:mariadb://localhost:3306/mydb") \
    .option("dbtable", "tweet_keywords") \
    .option("user", "ciori") \
    .option("password", "mariachepsw") \
    .load()

print(tweet_keywords_DF) '''

df = ss.createDataFrame([[1, "ciao"], [2, "ciaociao"], [1, "come"], [2, "stai"]])
df = df.repartition("_1")

def f(lines):
    print(lines)
    for line in lines:
        print(line)

df.foreachPartition(f)
