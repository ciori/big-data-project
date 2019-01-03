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
    .config("spark.driver.extraClassPath", "/home/ciori/Projects/big-data-project/mariadb-java-client-2.3.0.jar") \
    .master("local[*]") \
    .appName("Big Data Project Python - SQL") \
    .getOrCreate()

# SPARK CONTEXT
sc = ss.sparkContext

#schema = StructType().add("user_id", LongType(), False).add("keywords", StringType())

# read tweet_keywords table
tweet_keywords_DF = ss.read \
    .format("jdbc") \
    .option("url", "jdbc:mariadb://localhost:3306/mydb") \
    .option("dbtable", "test") \
    .option("user", "ciori") \
    .option("password", "mariachepsw") \
    .load()

tweet_keywords_DF.printSchema()

tweet_keywords_DF = tweet_keywords_DF.repartition("user_id")

# flatmap function
def init_counts(line):
    new_lines = []
    user_id = str(line[0])
    text = str(line[1])
    for keyword in text.split(" "):
        #new_line = [user_id + "_" + keyword, 1]
        new_line = [user_id, keyword, 1]
        #new_line = [keyword, 1]
        new_lines.append(new_line)
    return new_lines

# user profiling function
def userprofiling(user_keywords):
    #user_keywords_par = sc.parallelize(user_keywords)
    user_keywords = sc.createDataFrame(user_keywords)
    initial_keywords_counts = user_keywords.flatMap(lambda x: init_counts(x))
    keywords_counts = initial_keywords_counts.reduceByKey(add)

tweet_keywords_DF.foreachPartition(userprofiling)

'''# TEST ----------------------

tweet_keywords_DF = tweet_keywords_DF.repartition("user_id")

print("a")

def userprofiling(user_keywords):
    print("b")

tweet_keywords_DF.foreachPartition(userprofiling)'''

# TODO: passare a for che crea i dataframe -> userprofiling