import csv
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from operator import add

# SPARK SESSION
ss = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath", "/home/ciori/Projects/big-data-project/postgresql-42.2.5.jar") \
    .master("local[*]") \
    .appName("Big Data Project Python - SQL") \
    .getOrCreate()

# SPARK CONTEXT
sc = ss.sparkContext

# return list of user_id
user_ids = ss.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/tweetsdb") \
    .option("user", "postgres") \
    .option("query", "select distinct user_id from public.test") \
    .load()

# process tweets of each user
for user_id in user_ids.collect():

    # read keywords
    user_keywords = ss.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/tweetsdb") \
        .option("user", "postgres") \
        .option("query", "select * from public.test where user_id='" + str(user_id[0]) + "'") \
        .load()

    # flatmap function
    def init_counts(line):
        new_lines = []
        #user_id = str(line[0])
        text = str(line[1])
        for keyword in text.split(" "):
            #new_line = [user_id + "_" + keyword, 1]
            #new_line = [user_id, keyword, 1]
            new_line = [keyword, 1]
            new_lines.append(new_line)
        return new_lines

    #user_keywords_par = sc.parallelize(user_keywords)
    initial_keywords_counts = user_keywords.rdd.flatMap(lambda x: init_counts(x))
    keywords_counts = initial_keywords_counts.reduceByKey(add)

    print(keywords_counts.collect())