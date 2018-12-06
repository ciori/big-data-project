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

# flatmap function
def init_counts(line):
    new_lines = []
    user_id = str(line[0])
    text = str(line[1])
    for keyword in text.split(" "):
        new_line = [user_id + "_" + keyword, 1]
        new_lines.append(new_line)
    return new_lines

# PROCESSING
for partition in range(3, 4):#range(1, 29):
    chunksize = 1000000
    chunknumber = 0
    #for chunk in pd.read_csv("/media/fabio/Data2/full-database/pre-processed/tweet_keyword_" + str(partition) + ".csv", header=None, chunksize=chunksize):
    for chunk in pd.read_csv("/home/ciori/Unitn/Big Data/tweets-database/tweet-keyword/tweet_keywords_" + str(partition) + ".csv", header=None, chunksize=chunksize):
        chunknumber = chunknumber + 1
        keywords = sc.parallelize(chunk.values.tolist())
        counts_mono = keywords.flatMap(lambda x: init_counts(x))
        counts_reduced = counts_mono.reduceByKey(add)
        counts_reduced_sorted = counts_reduced.sortByKey()

        # test

        df = counts_reduced_sorted.toDF()
        df.write.csv(path="/home/ciori/Unitn/Big Data/tweets-database/keyword-count/keyword_count_" + str(partition) + ".csv", mode='append')

        '''# save results into mysql database
        print("---- toDF... ----")
        df = counts_reduced_sorted.toDF()
        df.printSchema()
        df = df.withColumnRenamed("_1", "user_keyword")
        df = df.withColumnRenamed("_2", "freq")
        print("---- tempview... ----")
        df.createOrReplaceTempView("keyword_count")
        url = "jdbc:mariadb://localhost:3306/mydb"
        properties = {"user": "ciori", "password": "mariachepsw"}
        print("---- write... ----")
        df.write.jdbc(url=url, table="keyword_count", mode="append", properties=properties)
        print("---- ...done----")'''

'''
        counts_reduced_sorted.saveAsTextFile("/media/fabio/Data2/full-database/keyword-count/keyword_count_" + str(partition) + "_" + str(chunknumber) + ".csv")
        counts_reduced_sorted.saveAsTextFile("/home/ciori/Unitn/Big Data/tweets-database/keyword-count/keyword_count_" + str(partition) + "_" + str(chunknumber) + ".csv")
        partition_csv = open("/media/fabio/Data2/full-database/keyword-count/keyword_count_" + str(partition) + ".csv", "a")
        partition_csv = open("/home/ciori/Unitn/Big Data/tweets-database/keyword-count/keyword_count_" + str(partition) + ".csv", "a")
        partition_reader = csv.writer(partition_csv)
        for chunk in range(1, chunknumber):
            for task_number in range(4):
                #chunk_csv = open("/media/fabio/Data2/full-database/keyword-count/keyword_count_" + str(partition) + "_" + str(chunknumber) + ".csv", "r")
                task_csv = open("/home/ciori/Unitn/Big Data/tweets-database/keyword-count/keyword_count_" + str(partition) + "_" + str(chunknumber) + ".csv/part-0000" + str(task_number), "r")
                task_reader = csv.reader(task_csv)
                for row in task_reader:
                    first = str(row[0]).replace("(", "").replace("'", "", 2)
                    second = str(row[1]).replace(")", "")
                    partition_reader.writerow([first, second])  
'''
