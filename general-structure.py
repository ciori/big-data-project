import csv
from pyspark import SparkContext
from operator import add

# Spark Context
sc = SparkContext("local[*]", "Big Data Project Python")

# Open tweet_text csv
#tweets_text_csv = open("/home/ciori/Unitn/Big Data/tweets-database/tweet_keywords_03.csv", "r")
tweets_text_csv = open("tweets_sample.csv", "r")
tweets_text_reader = csv.reader(tweets_text_csv)

# Open csv using textFile
#tweets_text_csv = sc.textFile("...")

tweets_text = sc.parallelize(tweets_text_reader)

print(tweets_text.collect())

# Function to initialize keywords counts
def init_counts(line):
    new_lines = []
    for keyword in line[1].split(" "):
        new_line = [line[0] + "_" + keyword, 1]
        new_lines.append(new_line)
    return new_lines

# FLATMAP: keywords counts intialization
users_keywords = tweets_text.flatMap(lambda x: init_counts(x))

print(users_keywords.collect())

# REDUCE: count keywords per user
users_keywords_counts = users_keywords.reduceByKey(add)

print(users_keywords_counts.collect())

# ------------------------------------------------------------------------------

# Sort by users
users_keywords_sorted = users_keywords_counts.sortByKey()

print(users_keywords_sorted.collect())

# Function to rebuild user_id, keywords...
def rebuild(line):
    count = str(line[1])
    keyword = str(line[0].split("_")[1])
    value = keyword + " " + count
    return [line[0].split("_")[0], [value]]

# MAP: rebuild entries -> user_id, keyword count
keywords_per_users = users_keywords_sorted.map(lambda x: rebuild(x))

print(keywords_per_users.collect())

# Function to group keywords for the same user
def group_keywords(x, y):
    keywords_group = []
    for keyword in x:
        keywords_group.append(keyword)
    for keyword in y:
        keywords_group.append(keyword)
    return keywords_group

# REDUCE: group keywords per same user
users_profiles_raw = keywords_per_users.reduceByKey(lambda x,y: group_keywords(x, y))

print(users_profiles_raw.collect())

# TODO: bigdata file?, sparkcontext configuration
