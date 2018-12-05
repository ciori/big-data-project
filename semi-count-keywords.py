import csv
import pandas as pd
from pyspark import SparkContext
from operator import add

'''
 Iput: all pre-processed csv files of tweets ["user_id", "list of keywords in the tweet"]
 Output: single csv file containing semi-sorted, semi-reduced counts of keywords ["user_id_keyword", "count"]
'''

# INITIALIZATION
sc = SparkContext("local[*]", "Big Data Project Python")

# FUNCTIONS
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
    chunknumber = 1
    for chunk in pd.read_csv("/media/fabio/Data2/full-database/pre-processed/tweet_keyword_" + str(partition) + ".csv", header=None, chunksize=chunksize):
        keywords = sc.parallelize(chunk.values.tolist())
        counts_mono = keywords.flatMap(lambda x: init_counts(x))
        counts_reduced = counts_mono.reduceByKey(add)
        counts_reduced_sorted = counts_reduced.sortByKey()
        counts_reduced_sorted.saveAsTextFile("/media/fabio/Data2/full-database/keyword-count/keyword_count_" + str(partition) + "_" + str(chunknumber) + ".csv")
        chunknumber = chunknumber + 1
    partition_csv = open("/media/fabio/Data2/full-database/keyword-count/keyword_count_" + str(partition) + ".csv", "a")
    partition_reader = csv.reader(partition_csv)
    for chunk in range(1, chunknumber):
        chunk_csv = open("/media/fabio/Data2/full-database/keyword-count/keyword_count_" + str(partition) + "_" + str(chunknumber) + ".csv", "r")
        chunk_reader = csv.reader(chunk_csv)
        for row in chunk_reader:
            partition_reader.writerow(row)