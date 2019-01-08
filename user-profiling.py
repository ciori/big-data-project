import csv
import datetime
import operator
from pyspark import SparkContext
from pyspark.sql import SparkSession

print("Starting at: " + str(datetime.datetime.now()))

# SPARK SESSION
ss = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath", "/home/ciori/Projects/big-data-project/postgresql-42.2.5.jar") \
    .master("local[4]") \
    .appName("Big Data Project Python - SQL") \
    .getOrCreate()

# SPARK CONTEXT
sc = ss.sparkContext

# set users profilings output csv
output_path = "/home/ciori/Unitn/Big Data/tweets-database/user-profile/user_profile_top_10.csv"
output_file = open(output_path, "a")
output_writer = csv.writer(output_file)

# get users ids intervals from csv
intervals_path = "/home/ciori/Unitn/Big Data/tweets-database/user-profile/users_intervals.csv"
intervals_file = open(intervals_path, "r")
intervals_reader = csv.reader(intervals_file)

# process each interval of user ids
iteration = 1
for interval in intervals_reader:
    
    print("Iteration " + str(iteration) + ", Interval: " + str(interval) + " started at " + str(datetime.datetime.now()))

    users_keywords = ss.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/tweetsdb") \
        .option("user", "postgres") \
        .option("query", "select * from public.users_keywords where user_id>='" + str(interval[0]) + "' and user_id<='" + str(interval[1]) + "'") \
        .load()
    
    users_keywords_rdd = users_keywords.rdd
    users_keywords_rdd = users_keywords_rdd.repartition(4)

    def init_keywords_and_counts(line):
        user_id = str(line[0])
        text = str(line[1])
        keywords_and_counts = []
        for keyword in text.split(" "):
            keyword_and_count = (keyword, 1)
            keywords_and_counts.append(keyword_and_count)
        return [user_id, keywords_and_counts]

    initial_keywords_and_counts = users_keywords_rdd.map(lambda x: init_keywords_and_counts(x))

    def count_keywords_top_10(list1, list2):
        # reduce keywords and counts
        reduced_list = dict([])
        for l in list1:
            if l[0] in reduced_list:
                reduced_list[l[0]] += l[1]
            else:
                reduced_list[l[0]] = l[1]
        for l in list2:
            if l[0] in reduced_list:
                reduced_list[l[0]] += l[1]
            else:
                reduced_list[l[0]] = l[1]
        # keep top 10
        ordered_list = list(reduced_list.items())
        ordered_list.sort(key=operator.itemgetter(1), reverse=True)
        top_10 = ordered_list[:10]
        return top_10

    users_profiles_top_10 = initial_keywords_and_counts.reduceByKey(lambda x,y: count_keywords_top_10(x, y))

    # save user profile into csv
    output_writer.writerows(users_profiles_top_10.collect())

    print("    done at: " + str(datetime.datetime.now()))
    iteration += 1

print("Finished at: " + str(datetime.datetime.now()))