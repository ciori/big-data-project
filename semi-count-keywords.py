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
chunksize = 1000000
chunknumber = 0
for chunk in pd.read_csv("/home/ciori/Unitn/Big Data/tweets-database/tweet-keyword/tweet_keywords_03.csv", header=None, chunksize=chunksize):
    keywords = sc.parallelize(chunk.values.tolist())
    counts_mono = keywords.flatMap(lambda x: init_counts(x))
    counts_reduced = counts_mono.reduceByKey(add)
    counts_reduced_sorted = counts_reduced.sortByKey()
    counts_reduced_sorted.saveAsTextFile("/home/ciori/Unitn/Big Data/tweets-database/keyword-count/keyword_count_03_" + str(chunknumber) + ".csv")
    chunknumber = chunknumber + 1

# TODO: complete "for" for all data_0x and then build them into one csv
